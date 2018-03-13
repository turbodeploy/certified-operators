
# export BIRT_HOME=/srv/birt-runtime
# export VMT_REPORTS_HOME=/srv/reports

DATE_SUBDIR=`date "+%Y-%m-%d"`
TARGET_DIR=$VMT_REPORTS_HOME/pdf_files/$DATE_SUBDIR

mkdir -m 775 $TARGET_DIR 2> /dev/null
if [ -f /etc/redhat-release ]
then 
  sudo chown apache.tomcat $TARGET_DIR 2> /dev/null
else 
  chown wwwrun:tomcat $TARGET_DIR 2> /dev/null
fi

$BIRT_HOME/ReportEngine/genReport.sh -m run --output $TARGET_DIR/$1.rptdocument $VMT_REPORTS_HOME/VmtReports/$1.rptdesign

# Render PDF
$BIRT_HOME/ReportEngine/genReport.sh -m render --format PDF --output $TARGET_DIR/$1.pdf $TARGET_DIR/$1.rptdocument

# Render Excel
$BIRT_HOME/ReportEngine/genReport.sh -m render --format XLSX --output $TARGET_DIR/$1.xlsx $TARGET_DIR/$1.rptdocument

# Cleanup
rm $TARGET_DIR/$1.rptdocument

chmod 644 $TARGET_DIR/$1.*

