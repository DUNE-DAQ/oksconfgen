from calendar import day_abbr
from curses import qiflush
import oksdbinterfaces
import os
import glob
from oksconfgen.assets import resolve_asset_file


def generate_readout(
    readoutmap,
    oksfile,
    include,
    segment,
    session,
    emulated_file_name="asset://?checksum=e96fd6efd3f98a9a3bfaba32975b476e",
):
    """Simple script to create an OKS configuration file for all
  ReadoutApplications defined in a readout map.

    The file will automatically include the relevant schema files and
  any other OKS files you specify. Any necessary objects not supplied
  by included files will be generated and saved in the output file.

   Example:
     generate_readoutOKS -i hosts \
       -i appdal/connections.data.xml -i appdal/moduleconfs \
       config/np04readoutmap.data.xml readoutApps.data.xml

   Will load hosts, connections and moduleconfs data files as well as
  the readoutmap (config/np04readoutmap.data.xml) and write the
  generated apps to readoutApps.data.xml.

     generate_readoutOKS --session --segment \
       -i appdal/fsm -i hosts \
       -i appdal/connections.data.xml -i appdal/moduleconfs  \
       config/np04readoutmap.data.xml np04readout-session.data.xml

   Will do the same but in addition it will generate a containing
  Segment for the apps and a containing Session for the Segment.

  NB: Currently FSM generation is not implemented so you must include
  an fsm file in order to generate a Segment

  """

    if not readoutmap.endswith(".data.xml"):
        readoutmap = readoutmap + ".data.xml"

    print(f"Readout map file {readoutmap}")

    includefiles = [
        "schema/coredal/dunedaq.schema.xml",
        "schema/appdal/application.schema.xml",
        "schema/appdal/trigger.schema.xml",
        "schema/appdal/fdmodules.schema.xml",
        readoutmap,
    ]

    searchdirs = [path for path in os.environ["DUNEDAQ_SHARE_PATH"].split(":")]
    searchdirs.append(os.path.dirname(oksfile))
    for inc in include:
        # print (f"Searching for {inc}")
        match = False
        inc = inc.removesuffix(".xml")
        if inc.endswith(".data"):
            sub_dirs = ["config", "data"]
        elif inc.endswith(".schema"):
            sub_dirs = ["schema"]
        else:
            sub_dirs = ["*"]
            inc = inc + "*"
        for path in searchdirs:
            # print (f"   {path}/{inc}.xml")
            matches = glob.glob(f"{inc}.xml", root_dir=path)
            if len(matches) == 0:
                for search_dir in sub_dirs:
                    # print (f"   {path}/{search_dir}/{inc}.xml")
                    matches = glob.glob(f"{search_dir}/{inc}.xml", root_dir=path)
                    for filename in matches:
                        if filename not in includefiles:
                            print(f"Adding {filename} to include list")
                            includefiles.append(filename)
                        else:
                            print(f"{filename} already in include list")
                        match = True
                        break
                    if match:
                        break
                if match:
                    break
            else:
                for filename in matches:
                    if filename not in includefiles:
                        print(f"Adding {filename} to include list")
                        includefiles.append(filename)
                    else:
                        print(f"{filename} already in include list")
                    match = True
                    break

        if not match:
            print(f"Error could not find include file for {inc}")
            return

    dal = oksdbinterfaces.dal.module("generated", includefiles[3])
    db = oksdbinterfaces.Configuration("oksconfig")
    if not oksfile.endswith(".data.xml"):
        oksfile = oksfile + ".data.xml"
    print(f"Creating OKS database file {oksfile}")
    db.create_db(oksfile, includefiles)

    rogs = db.get_dals(class_name="ReadoutGroup")
    hermes_controllers = db.get_dals(class_name="HermesController")

    if len(db.get_dals(class_name="LatencyBuffer")) > 0:
        print(f"Using predefined Latency buffers etc.")
        reqhandler = db.get_dal(
            class_name="RequestHandler", uid="def-data-request-handler"
        )
        latencybuffer = db.get_dal(class_name="LatencyBuffer", uid="def-latency-buf")
        linkhandler = db.get_dal(class_name="ReadoutModuleConf", uid="def-link-handler")
        tphandler = db.get_dal(class_name="ReadoutModuleConf", uid="def-tp-handler")
    else:
        print(f"Creating locally defined Latency buffers etc.")
        reqhandler = dal.RequestHandler("rh-1")
        db.update_dal(reqhandler)
        latencybuffer = dal.LatencyBuffer("lb-1")
        db.update_dal(latencybuffer)
        dataproc = dal.RawDataProcessor(
            "dataproc-1",
            max_ticks_tot=10000,
            algorithm="SimpleThreshold",
            threshold=1900,
            channel_map="PD2HDChannelMap",
        )
        db.update_dal(dataproc)
        linkhandler = dal.ReadoutModuleConf(
            "def-link-handler",
            template_for="FDDataLinkHandler",
            input_data_type="WIBEthFrame",
            request_handler=reqhandler,
            latency_buffer=latencybuffer,
            data_processor=dataproc,
        )
        db.update_dal(linkhandler)
        tphandler = dal.ReadoutModuleConf(
            "def-tp-handler",
            template_for="TriggerDataHandler",
            input_data_type="TriggerPrimitive",
            request_handler=reqhandler,
            latency_buffer=latencybuffer,
            data_processor=dataproc,
        )
        db.update_dal(tphandler)
    try:
        rule = db.get_dal(class_name="NetworkConnectionRule", uid="data-req-net-rule")
    except:
        # Failed to get rule, now we have to invent some
        netrules = generate_net_rules(dal, db)
    else:
        netrules = [rule]
        # Assume we have all the other rules we need
        for rule in ["tp-net-rule", "ts-net-rule", "ta-net-rule"]:
            netrules.append(db.get_dal(class_name="NetworkConnectionRule", uid=rule))

    try:
        rule = db.get_dal(
            class_name="QueueConnectionRule", uid="data-requests-queue-rule"
        )
    except:
        qrules = generate_queue_rules(dal, db)
    else:
        qrules = [rule]
        for rule in ["fa-queue-rule", "wib-eth-raw-data-rule", "tp-queue-rule"]:
            qrules.append(db.get_dal(class_name="QueueConnectionRule", uid=rule))

    hosts = []
    for host in db.get_dals(class_name="VirtualHost"):
        hosts.append(host.id)
    if "vlocalhost" not in hosts:
        cpus = dal.ProcessingResource("cpus", cpu_cores=[0, 1, 2, 3])
        db.update_dal(cpus)
        phdal = dal.PhysicalHost("localhost", contains=[cpus])
        db.update_dal(phdal)
        host = dal.VirtualHost("vlocalhost", runs_on=phdal, uses=[cpus])
        db.update_dal(host)
        hosts.append("vlocalhost")

    rohw = dal.RoHwConfig(f"rohw-{rogs[0].id}")
    db.update_dal(rohw)

    appnum = 0
    nicrec = None
    flxcard = None
    ruapps = []
    for rog in rogs:
        hostnum = appnum % len(hosts)
        # print(f"Looking up host[{hostnum}] ({hosts[hostnum]})")
        host = db.get_dal(class_name="VirtualHost", uid=hosts[hostnum])

        # Emulated stream
        if type(rog.contains[0]).__name__ == "ReadoutInterface":
            if nicrec == None:
                stream_emu = dal.StreamEmulationParameters(
                    "stream-emu",
                    data_file_name=resolve_asset_file(emulated_file_name),
                    input_file_size_limit=1000000,
                    set_t0=True,
                    random_population_size=100000,
                    frame_error_rate_hz=0,
                    generate_periodic_adc_pattern=True,
                    TP_rate_per_channel=1,
                )
                db.update_dal(stream_emu)
                print("Generating NICReceiverConf")
                nicrec = dal.NICReceiverConf(
                    f"nicrcvr-1",
                    template_for="FDFakeCardReader",
                    emulation_mode=1,
                    emulation_conf=stream_emu,
                )
                db.update_dal(nicrec)
            datareader = nicrec
        if type(rog.contains[0]).__name__ == "NICInterface":
            if nicrec == None:
                print("Generating NICReceiverConf")
                nicrec = dal.NICReceiverConf(f"nicrcvr-1", template_for="NICReceiver")
                db.update_dal(nicrec)
            datareader = nicrec
            hermes_app = dal.DaqApplication(
                f"hermes-{rog.id}", runs_on=host, modules=hermes_controllers
            )
            db.update_dal(hermes_app)
        if type(rog.contains[0]).__name__ == "FelixInterface":
            if flxcard == None:
                print("Generating Felix DataReaderConf")
                flxcard = dal.DataReaderConf(
                    f"flxConf-1", template_for="FelixCardReader"
                )
                db.update_dal(flxcard)
            datareader = flxcard

        ru = dal.ReadoutApplication(
            f"ru-{rog.id}",
            tp_source_id=appnum + 100,
            ta_source_id=appnum + 1000,
            runs_on=host,
            contains=[rog],
            network_rules=netrules,
            queue_rules=qrules,
            link_handler=linkhandler,
            tp_handler=tphandler,
            data_reader=datareader,
            uses=rohw,
        )
        appnum = appnum + 1
        print(f"{ru=}")
        db.update_dal(ru)
        ruapps.append(ru)

    if segment or session:
        fsm = db.get_dal(class_name="FSMconfiguration", uid="fsmConf-1")
        controller = dal.RCApplication("ru-controller", runs_on=host, fsm=fsm)
        db.update_dal(controller)
        seg = dal.Segment(f"ru-segment", controller=controller, applications=ruapps)
        db.update_dal(seg)

        if session:
            ro_map = db.get_dal(class_name="ReadoutMap", uid="readoutmap")
            detconf = dal.DetectorConfig("dummy-detector")
            db.update_dal(detconf)
            sessname = os.path.basename(readoutmap).removesuffix(".data.xml")
            sessiondal = dal.Session(
                f"{sessname}-session",
                segment=seg,
                detector_configuration=detconf,
                readout_map=ro_map,
            )
            db.update_dal(sessiondal)

    db.commit()
    return


def generate_net_rules(dal, db):
    print(f"Generating network rules")
    netrules = []
    dataservice = dal.Service("dataFragments")
    db.update_dal(dataservice)
    tpservice = dal.Service("triggerPrimitives")
    db.update_dal(tpservice)
    timeservice = dal.Service("timeSync")
    db.update_dal(timeservice)

    newdescr = dal.NetworkConnectionDescriptor(
        "fa-net-descr",
        uid_base="data_requests_for_",
        connection_type="kSendRecv",
        data_type="DataRequest",
        associated_service=dataservice,
    )
    db.update_dal(newdescr)
    newrule = dal.NetworkConnectionRule(
        "fa-net-rule", endpoint_class="FragmentAggregator", descriptor=newdescr
    )
    db.update_dal(newrule)
    netrules.append(newrule)

    newdescr = dal.NetworkConnectionDescriptor(
        "ta-net-descr",
        uid_base="ta_",
        connection_type="kPubSub",
        data_type="TriggerActivity",
        associated_service=dataservice,
    )
    db.update_dal(newdescr)
    newrule = dal.NetworkConnectionRule(
        "ta-net-rule", endpoint_class="DataSubscriber", descriptor=newdescr
    )
    db.update_dal(newrule)
    netrules.append(newrule)

    newdescr = dal.NetworkConnectionDescriptor(
        "tp-net-descr",
        uid_base="trigger_primitive_data_request",
        connection_type="kPubSub",
        data_type="TPSet",
        associated_service=tpservice,
    )
    db.update_dal(newdescr)
    newrule = dal.NetworkConnectionRule(
        "tp-net-rule", endpoint_class="FDDataLinkHandler", descriptor=newdescr
    )
    db.update_dal(newrule)
    netrules.append(newrule)

    newdescr = dal.NetworkConnectionDescriptor(
        "ts-net-descr",
        uid_base="timeSync",
        connection_type="kPubSub",
        data_type="TimeSync",
        associated_service=timeservice,
    )
    db.update_dal(newdescr)
    newrule = dal.NetworkConnectionRule(
        "ts-net-rule", endpoint_class="FDDataLinkHandler", descriptor=newdescr
    )
    db.update_dal(newrule)
    netrules.append(newrule)
    return netrules


def generate_queue_rules(dal, db):
    qrules = []
    newdescr = dal.QueueDescriptor(
        "dataRequest", queue_type="kFollySPSCQueue", data_type="DataRequest"
    )
    db.update_dal(newdescr)
    newrule = dal.QueueConnectionRule(
        "data-requests-queue-rule", destination_class="FDDataLinkHandler", descriptor=newdescr
    )
    db.update_dal(newrule)
    qrules.append(newrule)

    newdescr = dal.QueueDescriptor(
        "aggregatorInput", queue_type="kFollyMPMCQueue", data_type="Fragment"
    )
    db.update_dal(newdescr)
    newrule = dal.QueueConnectionRule(
        "fa-queue-rule",
        destination_class="FragmentAggregator",
        descriptor=newdescr,
    )
    db.update_dal(newrule)
    qrules.append(newrule)

    newdescr = dal.QueueDescriptor(
        "rawWIBInput", queue_type="kFollySPSCQueue", data_type="WIBEthFrame"
    )
    db.update_dal(newdescr)
    newrule = dal.QueueConnectionRule(
        "rawInputRule", destination_class="FDDataLinkHandler", descriptor=newdescr
    )
    db.update_dal(newrule)
    qrules.append(newrule)

    newdescr = dal.QueueDescriptor(
        "tpInput",
        queue_type="kFollyMPMCQueue",
        capacity=100000,
        data_type="TriggerPrimitive",
    )
    db.update_dal(newdescr)
    newrule = dal.QueueConnectionRule(
        "tpRule", destination_class="FDDataLinkHandler", descriptor=newdescr
    )
    db.update_dal(newrule)
    qrules.append(newrule)

    return qrules
