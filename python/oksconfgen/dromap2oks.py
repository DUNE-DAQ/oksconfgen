import oksdbinterfaces
import os
import json
import sys


def generate_hwmap(oksfile, n_streams, n_apps = 1, det_id = 3, app_host = "localhost",
                             eth_protocol = "udp", flx_mode = "fix_rate"):

    schemafiles = [
        "schema/coredal/dunedaq.schema.xml",
        "schema/appdal/application.schema.xml",
        "schema/appdal/fdmodules.schema.xml",
    ]
    dal = oksdbinterfaces.dal.module("generated", schemafiles[2])
    db = oksdbinterfaces.Configuration("oksconfig")
    db.create_db(oksfile, schemafiles)

    group_name = os.path.basename(oksfile).removesuffix(".data.xml")
    groups = []
    streams = []
    source_id = 0

    nic_stats_dal = dal.NICStatsConf(f"nicStats-{group_name}")
    db.update_dal(nic_stats_dal)
    nic_config_dal = dal.NICInterfaceConfiguration(
        f"nicConfig-{group_name}", stats_conf=nic_stats_dal
    )
    db.update_dal(nic_config_dal)

    stream_pars = dal.EthStreamParameters(
        f"pars",
        protocol=eth_protocol,
        mode=flx_mode,
        tx_hostname=app_host,
        tx_mac="00:00:00:00:00:00",
        tx_ip="0.0.0.0",
        # lcore = pars["lcore"],
        # rx_queue = pars["rx_queue"]
    )

    db.update_dal(stream_pars)

    for app in range(n_apps):

        for stream_no in range(n_streams):

            geo_dal = dal.GeoId(
                f"geioId-{source_id}",
                detector_id=det_id,
                crate_id=0,
                slot_id=app,
                stream_id=stream_no,
            )
            db.update_dal(geo_dal)
            stream = dal.DROStreamConf(
                f"DROStream-{source_id}",
                source_id=source_id,
                stream_params=stream_pars,
                geo_id=geo_dal,
            )
            db.update_dal(stream)
            streams.append(stream)
            source_id = source_id + 1

        print(f"New nic adding nic with id nic-{app}")
        nic_dal = dal.NICInterface(
            f"nic-{app}",
            rx_hostname=app_host,
            rx_mac="00:00:00:00:00:00",
            rx_ip="0.0.0.0",
            rx_iface=app,
            contains=streams,
            configuration=nic_config_dal,
        )
        db.update_dal(nic_dal)
        rogroup_dal = dal.ReadoutGroup(f"group-{app}", contains=[nic_dal])
        db.update_dal(rogroup_dal)
        groups.append(rogroup_dal)
        streams = []

    map_dal = dal.ReadoutMap("readoutmap", groups=groups)
    db.update_dal(map_dal)
    db.commit()


def dro_json_to_oks(jsonfile, oksfile, source_id_offset, nomap):
    """Simple script to convert a JSON readout map file to an OKS file."""

    group_name = os.path.basename(jsonfile).removesuffix(".json")
    if oksfile == "":
        oksfile = group_name + ".data.xml"

    print(
        f"Converting RO map from {jsonfile} to OKS in {oksfile} offsetting source_ids by {source_id_offset}"
    )

    with open(jsonfile) as f:
        jsonmap = json.loads(f.read())
        f.close()

    schemafiles = [
        "schema/coredal/dunedaq.schema.xml",
        "schema/appdal/application.schema.xml",
        "schema/appdal/fdmodules.schema.xml",
    ]
    dal = oksdbinterfaces.dal.module("generated", schemafiles[2])
    db = oksdbinterfaces.Configuration("oksconfig")
    db.create_db(oksfile, schemafiles)

    groups = []
    streams = []
    last_eth_pars = None
    last_felix_pars = None
    eth_streams_found = False
    flx_streams_found = False
    for entry in jsonmap:
        source_id = entry["src_id"] + source_id_offset
        if entry["kind"] == "eth":
            eth_source_id = source_id
            if not eth_streams_found:
                eth_streams_found = True
                nic_stats_dal = dal.NICStatsConf(f"nicStats-{group_name}")
                db.update_dal(nic_stats_dal)
                nic_config_dal = dal.NICInterfaceConfiguration(
                    f"nicConfig-{group_name}", stats_conf=nic_stats_dal
                )
                db.update_dal(nic_config_dal)

            pars = entry["parameters"]
            if last_eth_pars != None:
                # print(f"streams in nic {pars['rx_mac']} = {len(streams)}")
                if pars["rx_mac"] != last_eth_pars["rx_mac"]:
                    # if len(streams) > 0:
                    print(
                        f"New nic adding nic {last_eth_pars['rx_mac']} with id nic-{last_eth_source_id}"
                    )
                    nic_dal = dal.NICInterface(
                        f"nic-{last_eth_source_id}",
                        rx_hostname=last_eth_pars["rx_host"],
                        rx_mac=last_eth_pars["rx_mac"],
                        rx_ip=last_eth_pars["rx_ip"],
                        rx_iface=last_eth_pars["rx_iface"],
                        contains=streams,
                        configuration=nic_config_dal,
                    )
                    db.update_dal(nic_dal)
                    rogroup_dal = dal.ReadoutGroup(
                        f"group-{last_eth_source_id}", contains=[nic_dal]
                    )
                    db.update_dal(rogroup_dal)
                    groups.append(rogroup_dal)
                    streams = []
            if pars != last_eth_pars:
                # Only create a new dal object if the parameters are different to the last one
                stream_pars = dal.EthStreamParameters(
                    f"pars-{source_id}",
                    protocol=pars["protocol"],
                    mode=pars["mode"],
                    tx_hostname=pars["tx_host"],
                    tx_mac=pars["tx_mac"],
                    tx_ip=pars["tx_ip"],
                    # lcore = pars["lcore"],
                    # rx_queue = pars["rx_queue"]
                )

                db.update_dal(stream_pars)
                last_eth_pars = pars
                last_eth_source_id = source_id
        elif entry["kind"] == "flx":
            flx_source_id = source_id
            flx_streams_found = True
            pars = entry["parameters"]
            if not last_felix_pars == None:
                if (
                    pars["card"] != last_felix_pars["card"]
                    or pars["slr"] != last_felix_pars["slr"]
                ):
                    print(
                        f'Adding FelixInterface felix-{last_felix_source_id} slr={last_felix_pars["slr"]}'
                    )
                    felix_dal = dal.FelixInterface(
                        f"felix-{last_felix_source_id}",
                        card=last_felix_pars["card"],
                        slr=last_felix_pars["slr"],
                        contains=streams,
                    )
                    db.update_dal(felix_dal)
                    rogroup_dal = dal.ReadoutGroup(
                        f"group-{last_felix_source_id}", contains=[felix_dal]
                    )
                    db.update_dal(rogroup_dal)
                    groups.append(rogroup_dal)
                    streams = []
            stream_pars = dal.FelixStreamParameters(
                f"flxpars-{source_id}",
                protocol=pars["protocol"],
                mode=pars["mode"],
                link=pars["link"],
            )
            db.update_dal(stream_pars)
            last_felix_pars = pars
            last_felix_source_id = source_id
        else:
            raise RuntimeError(f'Unknown kind of readout {entry["kind"]}!')
        geo_id = entry["geo_id"]
        geo_dal = dal.GeoId(
            f"geioId-{source_id}",
            detector_id=geo_id["det_id"],
            crate_id=geo_id["crate_id"],
            slot_id=geo_id["slot_id"],
            stream_id=geo_id["stream_id"],
        )
        db.update_dal(geo_dal)
        stream = dal.DROStreamConf(
            f"DROStream-{source_id}",
            source_id=entry["src_id"],
            stream_params=stream_pars,
            geo_id=geo_dal,
        )
        db.update_dal(stream)
        streams.append(stream)
        last_source_id = source_id

    if eth_streams_found:
        print(
            f"Ending by adding nic {last_eth_pars['rx_mac']} with id nic-{eth_source_id}"
        )
        nic_dal = dal.NICInterface(
            f"nic-{eth_source_id}",
            rx_hostname=last_eth_pars["rx_host"],
            rx_mac=last_eth_pars["rx_mac"],
            rx_ip=last_eth_pars["rx_ip"],
            rx_iface=last_eth_pars["rx_iface"],
            contains=streams,
            configuration=nic_config_dal,
        )
        db.update_dal(nic_dal)
        rogroup_dal = dal.ReadoutGroup(f"group-{eth_source_id}", contains=[nic_dal])
        db.update_dal(rogroup_dal)
        groups.append(rogroup_dal)

    if flx_streams_found and len(streams) > 0:
        print(f"Adding final FelixInterface felix-{flx_source_id}")
        felix_dal = dal.FelixInterface(
            f"felix-{flx_source_id}",
            card=last_felix_pars["card"],
            slr=last_felix_pars["slr"],
            contains=streams,
        )
        db.update_dal(felix_dal)
        rogroup_dal = dal.ReadoutGroup(f"group-{flx_source_id}", contains=[felix_dal])
        db.update_dal(rogroup_dal)
        groups.append(rogroup_dal)

    if not nomap:
        map_dal = dal.ReadoutMap("readoutmap", groups=groups)
        db.update_dal(map_dal)
    db.commit()
