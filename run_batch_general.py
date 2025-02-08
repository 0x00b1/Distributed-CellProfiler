import json
import boto3
import botocore
import string
import posixpath
import argparse


class JobQueue():

    def __init__(self, name=None):
        self.sqs = boto3.resource("sqs")
        try:
            self.queue = self.sqs.get_queue_by_name(QueueName=name + "Queue")
        except botocore.exceptions.ClientError as error:
            if 'NonExistentQueue' in error.response['Error']['Code']:
                print (f"Queue {name}Queue does not exist.")
                exit()
        self.inProcess = -1
        self.pending = -1

    def scheduleBatch(self, data):
        msg = json.dumps(data)
        response = self.queue.send_message(MessageBody=msg)
        print("Batch sent. Message ID:", response.get("MessageId"))


def run_batch_general(
    step,  # (zproj, illum, qc, qc_persite, assaydev, assaydev_persite, or analysis)
    identifier="",  # (e.g. cpg0000-jump-pilot)
    batch="",  # (e.g. 2020_11_04_CPJUMP1)
    platelist=[],  # (e.g. ['Plate1','Plate2'])
    path_style="default",  # ("cpg" or "default")
    source="",  # (e.g. source_4, broad. Only with path_style=="cpg")
    plate_format="",  # (96 or 384. Overwrites rows and columns if passed. Not used by illum.)
    rows=list(string.ascii_uppercase)[0:16], # (Not used by illum.)
    columns=range(1, 25), # (Not used by illum.)
    wells="", # (explicitly list wells. Overwrites rows and columns if passed. Not used by illum. e.g. ['B3','C7'])
    sites=range(1, 10), # (Not used by illum, qc, or assaydev.)
    well_digit_pad=True,  # Set True to A01 well format name, set False to A1
    pipeline="",  # (overwrite default pipeline names)
    pipelinepath="",  # (overwrite default path to pipelines)
    inputpath="",  # (overwrite default path to input files)
    outputstructure="",  # (overwrite default output structures)
    outpath="",  # (overwrite default output paths)
    csvname="",  # (overwrite default load data csv name)
    datafilepath="",  # (overwrite default path to load data files)
    usebatch=False,  # (use h5 batch files instead of load data csv and cppipe files)
    batchfile="",  # (overwrite default batchfile name)
    batchpath="",  # (overwrite default path to batch files)
):

    # Two default file organization structures: cpg (for Cell Painting Gallery) and default
    path_dict = {
        "cpg": {
            "pipelinepath": posixpath.join(
                identifier, source, "workspace", "pipelines", batch
            ),
            "zprojoutpath": posixpath.join(
                identifier, source, "images", batch, "images_projected"
            ),
            "zprojoutputstructure": "Metadata_Plate",
            "illumoutpath": posixpath.join(
                identifier, source, "images", batch, "illum"
            ),
            "QCoutpath": posixpath.join(
                identifier, source, "workspace", "qc", batch, "results"
            ),
            "assaydevoutpath": posixpath.join(
                identifier, source, "workspace", "assaydev", batch
            ),
            "analysisoutpath": posixpath.join(
                identifier, source, "workspace", "analysis", batch
            ),
            "inputpath": posixpath.join(
                identifier, source, "workspace", "qc", batch, "rules"
            ),
            "datafilepath": posixpath.join(
                identifier, source, "workspace", "load_data_csv", batch
            ),
            "batchpath": "",
        },
        "default": {
            "pipelinepath": posixpath.join(
                "projects", identifier, "workspace", "pipelines", batch
            ),
            "zprojoutpath": posixpath.join("projects", identifier, batch, "images_projected"),
            "zprojoutputstructure": "Metadata_Plate",
            "illumoutpath": posixpath.join("projects", identifier, batch, "illum"),
            "QCoutpath": posixpath.join(
                "projects", identifier, "workspace", "qc", batch, "results"
            ),
            "assaydevoutpath": posixpath.join(
                "projects", identifier, "workspace", "assaydev", batch
            ),
            "analysisoutpath": posixpath.join(
                "projects", identifier, "workspace", "analysis", batch
            ),
            "inputpath": posixpath.join(
                "projects", identifier, "workspace", "qc", batch, "rules"
            ),
            "datafilepath": posixpath.join(
                "projects", identifier, "workspace", "load_data_csv", batch
            ),
            "batchpath": posixpath.join(
                "projects", identifier, "workspace", "batchfiles", batch
            ),
        },
    }
    if not pipelinepath:
        pipelinepath = path_dict[path_style]["pipelinepath"]
    if not batchpath:
        batchpath = path_dict[path_style]["batchpath"]
    if not inputpath:
        inputpath = path_dict[path_style]["inputpath"]
    if not datafilepath:
        datafilepath = path_dict[path_style]["datafilepath"]

    # Plate formatting
    if plate_format:
        if int(plate_format) == 384:
            rows = list(string.ascii_uppercase)[0:16]
            columns = range(1, 25)
        elif int(plate_format) == 96:
            rows = list(string.ascii_uppercase)[0:8]
            columns = range(1, 13)
        else:
            print(f"Unsupported plate format of {plate_format}.")
    if well_digit_pad:
        well_format = "02d"
    else:
        well_format = "01d"

    if step == "zproj":
        zprojqueue = JobQueue(f"{identifier}_Zproj")
        if not outputstructure:
            outputstructure = path_dict[path_style]["zprojoutputstructure"]
        if not outpath:
            outpath = path_dict[path_style]["zprojoutpath"]
        if not usebatch:
            if not pipeline:
                pipeline = "Zproj.cppipe"
            if not csvname:
                csvname = "load_data_unprojected.csv"

            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for eachsite in sites:
                                templateMessage_zproj = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={str(eachsite)}",
                                    "pipeline": posixpath.join(pipelinepath, pipeline),
                                    "output": outpath,
                                    "output_structure": outputstructure,
                                    "input": inputpath,
                                    "data_file": posixpath.join(
                                        datafilepath, plate, csvname
                                    ),
                                }
                                zprojqueue.scheduleBatch(templateMessage_zproj)
                else:
                    for eachwell in wells:
                        for eachsite in sites:
                            templateMessage_zproj = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={str(eachsite)}",
                                "pipeline": posixpath.join(pipelinepath, pipeline),
                                "output": outpath,
                                "output_structure": outputstructure,
                                "input": inputpath,
                                "data_file": posixpath.join(
                                    datafilepath, plate, csvname
                                ),
                            }
                            zprojqueue.scheduleBatch(templateMessage_zproj)                    
        else:
            if not batchfile:
                batchfile = "Batch_data_zproj.h5"
            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for eachsite in sites:
                                templateMessage_zproj = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={str(eachsite)}",
                                    "pipeline": posixpath.join(batchpath, batchfile),
                                    "output": outpath,
                                    "output_structure": outputstructure,
                                    "input": inputpath,
                                    "data_file": posixpath.join(batchpath, batchfile),
                                }
                                zprojqueue.scheduleBatch(templateMessage_zproj)
                else:
                    for eachwell in wells:
                        for eachsite in sites:
                            templateMessage_zproj = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={str(eachsite)}",
                                "pipeline": posixpath.join(batchpath, batchfile),
                                "output": outpath,
                                "output_structure": outputstructure,
                                "input": inputpath,
                                "data_file": posixpath.join(batchpath, batchfile),
                            }
                            zprojqueue.scheduleBatch(templateMessage_zproj)
        print("Z projection job submitted. Check your queue")

    elif step == "illum":
        illumqueue = JobQueue(f"{identifier}_Illum")
        if not outpath:
            outpath = path_dict[path_style]["illumoutpath"]
        if not usebatch:
            if not pipeline:
                pipeline = "illum.cppipe"
            if not csvname:
                csvname = "load_data.csv"

            for plate in platelist:
                templateMessage_illum = {
                    "Metadata": f"Metadata_Plate={plate}",
                    "pipeline": posixpath.join(pipelinepath, pipeline),
                    "output": outpath,
                    "input": inputpath,
                    "data_file": posixpath.join(datafilepath, plate, csvname),
                }

                illumqueue.scheduleBatch(templateMessage_illum)
        else:
            if not batchfile:
                batchfile = "Batch_data_illum.h5"
            for plate in platelist:
                templateMessage_illum = {
                    "Metadata": f"Metadata_Plate={plate}",
                    "pipeline": posixpath.join(batchpath, batchfile),
                    "output": outpath,
                    "input": inputpath,
                    "data_file": posixpath.join(batchpath, batchfile),
                }
                illumqueue.scheduleBatch(templateMessage_illum)

        print("Illum job submitted. Check your queue")

    elif step == "qc":
        qcqueue = JobQueue(f"{identifier}_QC")
        if not outpath:
            outpath = path_dict[path_style]["QCoutpath"]
        if not usebatch:
            if not pipeline:
                pipeline = "qc.cppipe"
            if not csvname:
                csvname = "load_data.csv"

            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            templateMessage_qc = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}}",
                                "pipeline": posixpath.join(pipelinepath, pipeline),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(datafilepath, plate, csvname),
                            }
                            qcqueue.scheduleBatch(templateMessage_qc)
                else:
                    for eachwell in wells:
                        templateMessage_qc = {
                            "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell}",
                            "pipeline": posixpath.join(pipelinepath, pipeline),
                            "output": outpath,
                            "input": inputpath,
                            "data_file": posixpath.join(datafilepath, plate, csvname),
                        }
                        qcqueue.scheduleBatch(templateMessage_qc)
        else:
            if not batchfile:
                batchfile = "Batch_data_qc.h5"
            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            templateMessage_qc = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}}",
                                "pipeline": posixpath.join(batchpath, batchfile),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(batchpath, batchfile),
                            }
                            qcqueue.scheduleBatch(templateMessage_qc)
                else:
                    for well in wells:
                        templateMessage_qc = {
                            "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell}",
                            "pipeline": posixpath.join(batchpath, batchfile),
                            "output": outpath,
                            "input": inputpath,
                            "data_file": posixpath.join(batchpath, batchfile),
                        }
                        qcqueue.scheduleBatch(templateMessage_qc)

        print("QC job submitted. Check your queue")

    elif step == "qc_persite":
        qcqueue = JobQueue(f"{identifier}_QC")
        if not outpath:
            outpath = path_dict[path_style]["QCoutpath"]
        if not usebatch:
            if not pipeline:
                pipeline = "qc.cppipe"
            if not csvname:
                csvname = "load_data.csv"

            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for eachsite in sites:
                                templateMessage_qc = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={str(eachsite)}",
                                    "pipeline": posixpath.join(pipelinepath, pipeline),
                                    "output": outpath,
                                    "input": inputpath,
                                    "data_file": posixpath.join(
                                        datafilepath, plate, csvname
                                    ),
                                }
                                qcqueue.scheduleBatch(templateMessage_qc)
                else:
                    for well in wells:
                        for eachsite in sites:
                            templateMessage_qc = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={str(eachsite)}",
                                "pipeline": posixpath.join(pipelinepath, pipeline),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(
                                    datafilepath, plate, csvname
                                ),
                            }
                            qcqueue.scheduleBatch(templateMessage_qc)
        else:
            if not batchfile:
                batchfile = "Batch_data_qc.h5"
            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for eachsite in sites:
                                templateMessage_qc = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={str(eachsite)}",
                                    "pipeline": posixpath.join(batchpath, batchfile),
                                    "output": outpath,
                                    "input": inputpath,
                                    "data_file": posixpath.join(batchpath, batchfile),
                                }
                                qcqueue.scheduleBatch(templateMessage_qc)
                else:
                    for well in wells:
                        for eachsite in sites:
                            templateMessage_qc = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={str(eachsite)}",
                                "pipeline": posixpath.join(batchpath, batchfile),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(batchpath, batchfile),
                            }
                            qcqueue.scheduleBatch(templateMessage_qc)

        print("QC job submitted. Check your queue")

    elif step == "assaydev":
        assaydevqueue = JobQueue(f"{identifier}_AssayDev")
        if not outpath:
            outpath = path_dict[path_style]["assaydevoutpath"]
        if not usebatch:
            if not pipeline:
                pipeline = "assaydev.cppipe"
            if not csvname:
                csvname = "load_data_with_illum.csv"

            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            templateMessage_ad = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}}",
                                "pipeline": posixpath.join(pipelinepath, pipeline),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(datafilepath, plate, csvname),
                            }
                            assaydevqueue.scheduleBatch(templateMessage_ad)
                else:
                    for well in wells:
                        templateMessage_ad = {
                            "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell}",
                            "pipeline": posixpath.join(pipelinepath, pipeline),
                            "output": outpath,
                            "input": inputpath,
                            "data_file": posixpath.join(datafilepath, plate, csvname),
                        }
                        assaydevqueue.scheduleBatch(templateMessage_ad)
        else:
            if not batchfile:
                batchfile = "Batch_data_assaydev.h5"
            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            templateMessage_ad = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}}",
                                "pipeline": posixpath.join(batchpath, batchfile),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(batchpath, batchfile),
                            }
                            assaydevqueue.scheduleBatch(templateMessage_ad)
                else:
                    for eachwell in wells:
                        templateMessage_ad = {
                            "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell}",
                            "pipeline": posixpath.join(batchpath, batchfile),
                            "output": outpath,
                            "input": inputpath,
                            "data_file": posixpath.join(batchpath, batchfile),
                        }
                        assaydevqueue.scheduleBatch(templateMessage_ad)

        print("AssayDev job submitted. Check your queue")

    elif step == "assaydev_persite":
        assaydevqueue = JobQueue(f"{identifier}_AssayDev")
        if not outpath:
            outpath = path_dict[path_style]["assaydevoutpath"]
        if not usebatch:
            if not pipeline:
                pipeline = "assaydev.cppipe"
            if not csvname:
                csvname = "load_data_with_illum.csv"

            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for site in sites:
                                templateMessage_ad = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={site}",
                                    "pipeline": posixpath.join(pipelinepath, pipeline),
                                    "output": outpath,
                                    "input": inputpath,
                                    "data_file": posixpath.join(datafilepath, plate, csvname),
                                }
                                assaydevqueue.scheduleBatch(templateMessage_ad)
                else:
                    for well in wells:
                        for site in sites:
                            templateMessage_ad = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={site}",
                                "pipeline": posixpath.join(pipelinepath, pipeline),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(datafilepath, plate, csvname),
                            }
                            assaydevqueue.scheduleBatch(templateMessage_ad)
        else:
            if not batchfile:
                batchfile = "Batch_data_assaydev.h5"
            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for site in sites:
                                templateMessage_ad = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={site}",
                                    "pipeline": posixpath.join(batchpath, batchfile),
                                    "output": outpath,
                                    "input": inputpath,
                                    "data_file": posixpath.join(batchpath, batchfile),
                                }
                                assaydevqueue.scheduleBatch(templateMessage_ad)
                else:
                    for eachwell in wells:
                        for site in sites:
                            templateMessage_ad = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={site}",
                                "pipeline": posixpath.join(batchpath, batchfile),
                                "output": outpath,
                                "input": inputpath,
                                "data_file": posixpath.join(batchpath, batchfile),
                            }
                            assaydevqueue.scheduleBatch(templateMessage_ad)

        print("AssayDev job submitted. Check your queue")


    elif step == "analysis":
        analysisqueue = JobQueue(f"{identifier}_Analysis")
        if not outputstructure:
            outputstructure = (
                "Metadata_Plate/analysis/Metadata_Plate-Metadata_Well-Metadata_Site"
            )
        if not outpath:
            outpath = path_dict[path_style]["analysisoutpath"]
        if not usebatch:
            if not pipeline:
                pipeline = "analysis.cppipe"
            if not csvname:
                csvname = "load_data_with_illum.csv"
            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for eachsite in sites:
                                templateMessage_analysis = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={str(eachsite)}",
                                    "pipeline": posixpath.join(pipelinepath, pipeline),
                                    "output": outpath,
                                    "output_structure": outputstructure,
                                    "input": inputpath,
                                    "data_file": posixpath.join(
                                        datafilepath, plate, csvname
                                    ),
                                }
                                analysisqueue.scheduleBatch(templateMessage_analysis)
                else:
                    for eachwell in wells:
                        for eachsite in sites:
                            templateMessage_analysis = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={str(eachsite)}",
                                "pipeline": posixpath.join(pipelinepath, pipeline),
                                "output": outpath,
                                "output_structure": outputstructure,
                                "input": inputpath,
                                "data_file": posixpath.join(
                                    datafilepath, plate, csvname
                                ),
                            }
                            analysisqueue.scheduleBatch(templateMessage_analysis)
        else:
            if not batchfile:
                batchfile = "Batch_data_analysis.h5"
            for plate in platelist:
                if all(len(ele) == 0 for ele in wells):
                    for eachrow in rows:
                        for eachcol in columns:
                            for eachsite in sites:
                                templateMessage_analysis = {
                                    "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachrow}{int(eachcol):{well_format}},Metadata_Site={str(eachsite)}",
                                    "pipeline": posixpath.join(batchpath, batchfile),
                                    "output": outpath,
                                    "output_structure": outputstructure,
                                    "input": inputpath,
                                    "data_file": posixpath.join(batchpath, batchfile),
                                }
                                analysisqueue.scheduleBatch(templateMessage_analysis)
                else:
                    for eachwell in wells:
                        for eachsite in sites:
                            templateMessage_analysis = {
                                "Metadata": f"Metadata_Plate={plate},Metadata_Well={eachwell},Metadata_Site={str(eachsite)}",
                                "pipeline": posixpath.join(batchpath, batchfile),
                                "output": outpath,
                                "output_structure": outputstructure,
                                "input": inputpath,
                                "data_file": posixpath.join(batchpath, batchfile),
                            }
                            analysisqueue.scheduleBatch(templateMessage_analysis)

        print("Analysis job submitted. Check your queue")

    else:
        print(f"Step {step} not supported.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make jobs for Distributed-CellProfiler"
    )
    parser.add_argument(
        "step",
        help="Step to make jobs for. Supported steps are zproj, illum, qc, qc_persite, assaydev, assaydev_persite, analysis",
    )
    parser.add_argument("identifier", help="Project identifier")
    parser.add_argument("batch", help="Name of batch")
    parser.add_argument("platelist", type=lambda s: list(s.split(",")), help="List of plates to process")
    parser.add_argument(
        "--path-style",
        dest="path_style",
        default="default",
        help="Style of input/output path. default or cpg (for Cell Painting Gallery structure).",
    )
    parser.add_argument(
        "--source",
        dest="source",
        default="",
        help="For Cell Painting Gallery, what is the source (nesting under project identifier).",
    )
    parser.add_argument(
        "--plate-format",
        dest="plate_format",
        default="",
        help="Plate format. Suppports 384 or 96. Auto-generates rows and columns and will overwrite --rows and --columns.",
    )
    parser.add_argument(
        "--rows",
        dest="rows",
        type=lambda s: list(s.split(",")),
        default="A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P",
        help="List of rows to process",
    )
    parser.add_argument(
        "--columns",
        dest="columns",
        type=lambda s: list(s.split(",")),
        default="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24",
        help="List of rows to process",
    )
    parser.add_argument(
        "--wells",
        dest="wells",
        type=lambda s: list(s.split(",")),
        default="",
        help="Explicit list of rows to process. Will overwrite --rows and --columns.",
    )
    parser.add_argument(
        "--sites",
        dest="sites",
        type=lambda s: list(s.split(",")),
        default="1,2,3,4,5,6,7,8,9",
        help="List of site to process",
    )
    parser.add_argument(
        "--no-well-digit-pad",
        dest="well_digit_pad",
        action="store_false",
        default=True,
        help="Format wells with padding e.g. A01",
    )
    parser.add_argument(
        "--pipeline",
        dest="pipeline",
        default="",
        help="Name of the pipeline to overwrite defaults of Zproj.cppipe, illum.cppipe, qc.cppipe, assaydev.cppipe, analysis.cppipe.",
    )
    parser.add_argument(
        "--pipeline-path",
        dest="pipelinepath",
        default="",
        help="Overwrite default path to pipelines.",
    )
    parser.add_argument(
        "--input-path",
        dest="inputpath",
        default="",
        help="Overwrite default path to input files.",
    )
    parser.add_argument(
        "--output-structure",
        dest="outputstructure",
        default="",
        help="Overwrites default outuput structure. Supported for zproj and analysis.",
    )
    parser.add_argument(
        "--output-path",
        dest="outpath",
        default="",
        help="Overwrites default outuput path.",
    )
    parser.add_argument(
        "--datafile-name",
        dest="csvname",
        default="",
        help="Name of load data .csv. Overwrites default of load_data.csv (illum), load_data_with_illum.csv (assaydev, qc, qc_persite, analysis) and load_data_unprojected.csv (Zproj).",
    )
    parser.add_argument(
        "--datafile-path",
        dest="datafilepath",
        default="",
        help="Overwrite default path to load data files.",
    )
    parser.add_argument(
        "--use-batch",
        dest="usebatch",
        action="store_true",
        default=False,
        help="Use CellProfiler h5 batch files instead of separate .cppipe and load_data.csv files. Supported for default, not cpg structure.",
    )
    parser.add_argument(
        "--batchfile-name",
        dest="batchfile",
        default="",
        help="Name of h5 batch file (if using). Overwrites defaults.",
    )
    parser.add_argument(
        "--batchfile-path",
        dest="batchpath",
        default="",
        help="Overwrite default path to h5 batch files.",
    )
    args = parser.parse_args()

    run_batch_general(
        args.step,
        path_style=args.path_style,
        identifier=args.identifier,
        batch=args.batch,
        platelist=args.platelist,
        source=args.source,
        plate_format=args.plate_format,
        rows=args.rows,
        columns=args.columns,
        wells=args.wells,
        sites=args.sites,
        well_digit_pad=args.well_digit_pad,
        pipeline=args.pipeline,
        pipelinepath=args.pipelinepath,
        inputpath=args.inputpath,
        outputstructure=args.outputstructure,
        outpath=args.outpath,
        csvname=args.csvname,
        datafilepath=args.datafilepath,
        usebatch=args.usebatch,
        batchfile=args.batchfile,
        batchpath=args.batchpath,
    )
