domain = "WestAfrica"
subdomain = "Regional"
model_resolution = "1km"
systemModel = "crest"
systemTimestep = 60 #in minutes

# Coordinates used for generating Nowcast / QPF files.
# For ML-based nowcasting, these coordinates should cover a region of size 518 x 360 pixels.
xmin = -21.4
xmax = 30.4
ymin = -2.9
ymax = 33.1
nowcast_model_name = "convlstm" 
systemName = systemModel.upper() + " " + domain.upper() + " " + subdomain.upper()
ef5Path = "/Dedicated/Humberto/EF5Binary/EF5/bin/ef5" 
statesPath = "states/"
precipFolder = "precip/"
precipEF5Folder = "precipEF5/"
modelStates = ["crest_SM", "kwr_IR", "kwr_pCQ", "kwr_pOQ"]
templatePath = "templates/"
templates = "ef5_control_template.txt"
dataPath = "outputs/"
qpf_store_path = 'qpf_store/'
tmpOutput = dataPath + "tmp_output_" + systemModel + "/"

# QPE source configuration.
# Options: "IMERG" (default), "HSAF"
qpe_source = "IMERG"

# HSAF credentials/settings (required only when qpe_source == "HSAF")
hsaf_ftp_user = "naman-mehta@uiowa.edu"
hsaf_ftp_pass = "change_me1234"
hsaf_latency_minutes = 20

#Alerts configuration
SEND_ALERTS = False
smtp_server = "smtp.gmail.com"
smtp_port = 587
account_address = "model_alerts@gmail.com"
account_password = "supersecurepassword9000"
alert_sender = "Real Time Model Alert" # can also be the same as account_address
alert_recipients = ["fixer1@company.com", "fixer2@company.com", "panic@company.com",...]
copyToWeb = False

#Simulation times 
"""
If Hindcast and LR_mode is True, user MUST define StartLRtime, EndLRTime, LR_timestep,GFS_archive_path
If running in operational mode (Hindcast False) and LR_mode = True, user only have to define LR_timestep, GFS_archive_path
"""
HindCastMode = False 
HindCastDate = "2024-07-04 09:00" #"%Y-%m-%d %H:%M" UTC

run_LR = True
StartLRtime = "2024-07-04 11:00" #"%Y-%m-%d %H:%M" UTC. Date of first QPF file
EndLRTime = "2024-07-04 18:00" #"%Y-%m-%d %H:%M" UTC. Date of last QPF file
LR_timestep = "60u"
QPF_archive_path = "qpf_store/archive/"  # legacy; kept for back-compat

# WRF configuration (used when run_LR=True).
# Set WRF_archive_path to the folder containing WRF netCDF files.
# Leave empty ("") to skip WRF and fall back directly to GFS.
WRF_archive_path = ""                               # e.g. "/data/wrf_output/"
WRF_var_name = "PREC_ACC_C"                         # precipitation variable name in WRF netCDFs
WRF_filename_template = "PREC_d01_YYYY-MM-DD_HH_mm_SS.nc"  # WRF filename pattern

# GFS configuration (used when run_LR=True and WRF not available).
# GFS tifs are stored here persistently and reused across cycles.
GFS_precip_path = "precip/GFS/"                     # persistent GFS tif archive

# Email associated to GPM account
email_gpm = 'vrobledodelgado@uiowa.edu'
server = 'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/'
