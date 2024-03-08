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

