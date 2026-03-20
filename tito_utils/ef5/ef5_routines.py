import os
import shutil
import re
import glob
from shutil import rmtree
import datetime
from datetime import timedelta
from multiprocessing.pool import ThreadPool
import subprocess
from tito_utils.file_utils.file_handling import is_non_zero_file, mkdir_p
from tito_utils.ef5.alerts import send_mail


def _apply_hsaf_control_overrides(lines):
    """Adjust generated EF5 control lines for HSAF forcing.

    - Comment the full IMERG forcing block.
    - Insert HSAF forcing block right after IMERG block.
    - In Task Simulation_QPE and Task Simulation_QPF, switch PRECIP to HSAF and TIMESTEP to 10u.
    """
    out = []
    i = 0
    inserted_hsaf_block = False
    in_qpe_task = False
    in_qpf_task = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Track whether we are inside Task Simulation_QPE block.
        if stripped == "[Task Simulation_QPE]":
            in_qpe_task = True
            in_qpf_task = False
        elif stripped == "[Task Simulation_QPF]":
            in_qpf_task = True
            in_qpe_task = False
        elif stripped.startswith("[") and stripped != "[Task Simulation_QPE]":
            in_qpe_task = False
            in_qpf_task = False

        # Comment IMERG forcing block and add HSAF block below it.
        if stripped == "[PrecipForcing IMERG]":
            while i < len(lines):
                block_line = lines[i]
                block_stripped = block_line.strip()
                if i > 0 and block_stripped.startswith("[") and block_stripped != "[PrecipForcing IMERG]":
                    break
                if block_line.lstrip().startswith("#"):
                    out.append(block_line)
                else:
                    out.append("#" + block_line)
                i += 1

            if not inserted_hsaf_block:
                out.extend([
                    "[PrecipForcing HSAF]\n",
                    "TYPE=TIF\n",
                    "UNIT=mm/h\n",
                    "FREQ=10u\n",
                    "LOC=precipEF5/\n",
                    "NAME=h40_YYYYMMDD_HHUU_fdk.tif\n",
                    "\n",
                ])
                inserted_hsaf_block = True
            continue

        if (in_qpe_task or in_qpf_task) and stripped.startswith("PRECIP="):
            out.append("PRECIP=HSAF\n")
            i += 1
            continue

        if (in_qpe_task or in_qpf_task) and stripped.startswith("TIMESTEP="):
            out.append("TIMESTEP=10u\n")
            i += 1
            continue

        out.append(line)
        i += 1

    return out

def rename_ef5_precip(precipEF5Folder, precipFolder, qpe_source="IMERG"): 
    """
    Copy precipitation TIFs into precipEF5Folder to be ingested by EF5.
    Clears precipEF5Folder first so files from a previous run (different QPE
    source or different cycle) never mix with the current run's files.
    For IMERG: scans precipFolder only.
    For HSAF: also scans precipFolder/_hsaf_raw/ since that is where converted TIFs live.
    """
    # Clear the folder before populating it so no stale files remain.
    for stale in glob.glob(os.path.join(precipEF5Folder, "*.tif")):
        try:
            os.remove(stale)
        except Exception as e:
            print(f"Warning: could not remove stale precipEF5 file {stale}: {e}")

    search_dirs = [precipFolder]
    if str(qpe_source).upper() == "HSAF":
        hsaf_raw = os.path.join(precipFolder, "_hsaf_raw")
        if os.path.isdir(hsaf_raw):
            search_dirs.append(hsaf_raw)

    for search_dir in search_dirs:
        for filename in os.listdir(search_dir):
            if filename.endswith('.tif'):
                source_file = os.path.join(search_dir, filename)
                dest_file = os.path.join(precipEF5Folder, filename)
                try:
                    shutil.copy(source_file, dest_file)
                except PermissionError as e:
                    print(f"PermissionError: {e}")
    for filename2 in os.listdir(precipEF5Folder):
        if 'qpf' in filename2 and filename2.endswith('.tif'):
            new_filename = filename2.replace('qpf', 'qpe')
            source_file = os.path.join(precipEF5Folder, filename2)
            dest_file = os.path.join(precipEF5Folder, new_filename)
            try:
                os.rename(source_file, dest_file)
            except PermissionError as e:
                print(f"PermissionError: {e}")


def find_available_states(statesPath, modelStates, systemStartTime, failTime):
    """
    Look for the set of most recent states available.
    
    """
    foundAllStates = False
    realSystemStartTime = systemStartTime

    print("    Looking for states.")

    # Iterate over all necessary states and check if they're available for the current run
    # Only go back up to 6 hours, in 30min decrements
    while not foundAllStates and realSystemStartTime > failTime:
        foundAllStates = True
        for state in modelStates:
            state_path = f"{statesPath}{state}_{realSystemStartTime.strftime('%Y%m%d_%H%M')}.tif"
            if not is_non_zero_file(state_path):
                print(f"    Missing start state: {state_path}")
                foundAllStates = False
        if not foundAllStates:
            realSystemStartTime -= timedelta(minutes=30)

    return foundAllStates, realSystemStartTime


def send_state_alerts(foundAllStates,realSystemStartTime,systemStartTime,currentTime,systemName,SEND_ALERTS,alert_recipients, smtp_config):
    """
    Sends alert emails if necessary based on the availability of model states.

    Args:
        foundAllStates (bool): whether all required states were found
        realSystemStartTime (datetime): actual start time used for the simulation
        systemStartTime (datetime): originally planned system start time
        currentTime (datetime): current system time
        systemName (str): name of the system sending the alert
        SEND_ALERTS (bool): whether to send email alerts or not
        alert_recipients (list): list of email addresses to notify
        smtp_config (dict): configuration dictionary containing:
            - smtp_server (str)
            - smtp_port (int)
            - account_address (str)
            - account_password (str)
            - alert_sender (str)
    """
    # Exit early if email alerts are disabled
    if not SEND_ALERTS:
        return

    # If no valid states were found, notify about a cold start
    if not foundAllStates:
        subject = f"{systemName} failed for {currentTime.strftime('%Y%m%d_%H%M')}"
        message = (
            f"Missing states from {realSystemStartTime.strftime('%Y%m%d_%H%M')} "
            f"to {systemStartTime.strftime('%Y%m%d_%H%M')}. Starting model with cold states."
        )
    
    # If older states had to be used, notify about it
    elif realSystemStartTime != systemStartTime:
        subject = f"{systemName} warning for {currentTime.strftime('%Y%m%d_%H%M')}"
        message = (
            f"Using states from {realSystemStartTime.strftime('%Y%m%d_%H%M')} "
            f"instead of {systemStartTime.strftime('%Y%m%d_%H%M')}."
        )
    
    # If states were found and up to date, no alert needed
    else:
        return

    # Send the email to each recipient in the list
    for recipient in alert_recipients:
        send_mail(
            smtp_server=smtp_config['smtp_server'],
            smtp_port=smtp_config['smtp_port'],
            account_address=smtp_config['account_address'],
            account_password=smtp_config['account_password'],
            sender=smtp_config['alert_sender'],
            to=recipient,
            subject=subject,
            text=message
        )

def write_control_file(tmpOutput, dataPath, subdomain, systemModel,templatePath, template, statesPath, realSystemStartTime, systemStartLRTime, systemWarmEndTime, systemStateEndTime, systemEndTime, LR_TimeStep, LR_run, statesFound, qpe_source="IMERG", qpf_source="GFS"):
    # Clean up "Hot" folders
    # Delete the previously existing "Hot" folders, ignore error if it doesn't exist
    rmtree(tmpOutput, ignore_errors=1)
    rmtree(dataPath, ignore_errors=1)
    # Create the "Hot" folder for the current run
    mkdir_p(tmpOutput)
    mkdir_p(dataPath)  
    # Create the control files for both subdomains
    # Define the control file path to create
    controlFile = tmpOutput + "WA_" + subdomain + "_" + systemModel + ".txt"
    fOut = open(controlFile, "w")

    # Create a control file with updated fields
    rendered_lines = []
    for line in open(templatePath + template).readlines():
        line = re.sub('{OUTPUTPATH}', tmpOutput, line)
        line = re.sub('{STATESPATH}', statesPath, line)
        line = re.sub('{TIMEBEGIN}', realSystemStartTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMEWARMEND}', systemWarmEndTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMESTATE}', systemStateEndTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMEEND}', systemEndTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMEBEGINLR}', systemStartLRTime.strftime('%Y%m%d%H%M'), line)
        line = re.sub('{TIMESTEPLR}', LR_TimeStep, line)
        line = re.sub('{SYSTEMMODEL}', systemModel, line)
        
        if "task=Simulation_QPE" in line:
            if LR_run:                      # QPF mode
                line = "#task=Simulation_QPE\n"   # comment QPE
        elif "task=Simulation_QPF" in line:
            if LR_run:
                line = "task=Simulation_QPF\n"    # uncomment QPF
            else:
                line = "#task=Simulation_QPF\n"   # comment QPF

        # Switch PRECIPFORECAST to the active QPF source (GFS or WRF)
        if LR_run and "PRECIPFORECAST=" in line and not line.lstrip().startswith('#'):
            line = re.sub(r'PRECIPFORECAST=\w+', f'PRECIPFORECAST={qpf_source.upper()}', line)

        # If valid states are found, do not specify warm-up in control file
        if statesFound and "TIME_WARMEND=" in line:
            if not line.lstrip().startswith('#'):
                line = "#" + line

        rendered_lines.append(line)

    if str(qpe_source).upper() == "HSAF":
        rendered_lines = _apply_hsaf_control_overrides(rendered_lines)

    for line in rendered_lines:
        fOut.write(line)
        
    fOut.close()
    return controlFile

def run_EF5(ef5Path, hot_folder_path, control_file, log_file):
    """
    Run EF5 as a subprocess call
    Arguments:
        ef5Path {str} -- Path to EF5 binary
        hot_folder_path {str} -- Path to the current run's "hot" foler
        control_file {str} -- path to the control file fir the simulation
        log_file {str} -- path to the log file for this run
    """
    subprocess.call(ef5Path + " " + control_file + " > " + hot_folder_path + log_file, shell=True)


def _rename_outputs_with_timestamp(hot_folder_path: str, timestamp_str: str) -> None:
    """Rename EF5 outputs in the hot folder to use a unified timestamp.

    - maxq.*, maxunitq.*, qpeaccum.*, qpfaccum.* -> base.{timestamp}.tif
    - ts.*.csv -> ts.*.{timestamp}.csv
    Log file naming is handled via the EF5 invocation (redirect target).
    """
    bases = ["maxq", "maxunitq", "qpeaccum", "qpfaccum", "maxsm"]
    for base in bases:
        pattern = os.path.join(hot_folder_path, f"{base}.*.tif")
        matches = sorted(glob.glob(pattern))
        if not matches:
            continue
        # Prefer the newest file in case multiple exist
        latest = max(matches, key=lambda p: os.path.getmtime(p))
        new_name = os.path.join(hot_folder_path, f"{base}.{timestamp_str}.tif")
        try:
            if os.path.abspath(latest) != os.path.abspath(new_name):
                if os.path.exists(new_name):
                    os.remove(new_name)
                os.rename(latest, new_name)
        except Exception as e:
            print(f"Warning: could not rename {latest} -> {new_name}: {e}")

    # Timeseries CSVs
    for csv_path in glob.glob(os.path.join(hot_folder_path, "ts.*.csv")):
        root, ext = os.path.splitext(csv_path)
        new_name = f"{root}.{timestamp_str}{ext}"
        try:
            if os.path.abspath(csv_path) != os.path.abspath(new_name):
                if os.path.exists(new_name):
                    os.remove(new_name)
                os.rename(csv_path, new_name)
        except Exception as e:
            print(f"Warning: could not rename {csv_path} -> {new_name}: {e}")


def run_ef5_simulation(ef5Path, tmpOutput, controlFile, output_timestamp_str):
    # Use timestamped log name
    log_name = f"ef5.{output_timestamp_str}.log"
    args = [ef5Path, tmpOutput, controlFile, log_name]
    tp = ThreadPool(1)
    tp.apply_async(run_EF5, args)
    tp.close()
    tp.join()

    # Rename generated outputs to use the requested timestamp
    _rename_outputs_with_timestamp(tmpOutput, output_timestamp_str)

    # cleaning EF5 precipitation for next cycle
    for f in glob.glob("precipEF5/*"):
        os.remove(f)

 
def prepare_ef5(precipEF5Folder, precipFolder, statesPath, modelStates, 
    systemStartTime, failTime, currentTime, systemName, SEND_ALERTS, 
    alert_recipients, smtp_config, tmpOutput, dataPath, 
    subdomain, systemModel, templatePath, template, systemStartLRTime, 
    systemWarmEndTime, systemStateEndTime, systemEndTime, LR_TimeStep, LR_run, qpe_source="IMERG", qpf_source="GFS"):

    #copying precip files into folder 
    rename_ef5_precip(precipEF5Folder, precipFolder, qpe_source) 

    # Check to see if all the states for the current time step are available: ["crest_SM", "kwr_IR", "kwr_pCQ", "kwr_pOQ"]
    # If not then search for previous ones

    foundAllStates, realSystemStartTime = find_available_states(statesPath, modelStates, systemStartTime, failTime)

    # send alerts if needed 
    send_state_alerts(foundAllStates, realSystemStartTime, systemStartTime,
                      currentTime, systemName, SEND_ALERTS,
                      alert_recipients, smtp_config)
                     
    print(" ")
    print("    Writting control file.")

    controlFile = write_control_file(tmpOutput, dataPath, subdomain, systemModel, 
    templatePath, template, statesPath, realSystemStartTime, systemStartLRTime, 
    systemWarmEndTime, systemStateEndTime, systemEndTime, LR_TimeStep, LR_run, foundAllStates, qpe_source, qpf_source)

    """
    # If data assimilation if being used for CREST, clean up previous data assimilation logs
    #To do: Verify against EF5 control file - when this functionality is needed
    if DATA_ASSIMILATION and systemModel=="crest":
        # Data assimilation output files
        for log in assimilationLogs:
            if is_non_zero_file(assimilationPath + log) == True:
                remove(assimilationPath + log)
    """
    return realSystemStartTime, controlFile
