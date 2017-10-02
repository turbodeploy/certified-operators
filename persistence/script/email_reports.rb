require 'fileutils'
require 'pdf_generators'
require 'rbconfig'


def environment_checks_pass()
  # unless ENV['VMT_REPORTS_HOME']
    # $logger.error("#{Time.now.to_s} ERROR: VMT_REPORTS_HOME env var not defined")
    # return false
  # end
  return true
end

def ok_to_send(subscription)
  return true if subscription.period == "Daily"
  return true if subscription.period == "Weekly"  && subscription.day_type.to_s.include?(Date::ABBR_DAYNAMES[Date.today.wday].to_s)
  return true if subscription.period == "Monthly" && Date.today.day == 1
  return false
end

# ---

def custom_rept_file(rept)
  return "/tmp/#{rept.title.to_s.gsub(/\W/,'')}.pdf"
end

def prepare_custom_report_pdf_file(rept)
  return if rept.is_obsolete?

  port = ":3000"

  cmd = "curl -s -o #{custom_rept_file(rept)} http://localhost#{port}/persistence/user_reports/as_pdf/#{rept.id}"
  unless system(cmd)
    $logger.error("#{Time.now.to_s} ERROR: Failure invoking: #{cmd}")
  end
end

def prepare_on_demand_report_pdf_file(subscription,rept)
  entity = Entity.find_by_uuid(subscription.obj_uuid)
  return "/tmp/no_such_object" unless entity  # this will happen if entities are off line or removed

  if rept.scope_type == 'Instance' then
    pdf_gtor = PdfGeneratorForSvcEntity.new(entity.display_name.to_s, rept.obj_type, subscription.obj_uuid.to_s)
  else
    entity_name = entity.display_name.to_s.gsub("'","''").gsub("\\","\\\\\\\\")
    pdf_gtor = PdfGeneratorForGroup.new(entity_name.gsub("\\","\\\\\\\\"), rept.obj_type, subscription.obj_uuid.to_s)
  end

  rept_type = rept.filename.to_s.gsub(/^\w\w_(group_)?/,'')
  pdf_file_path = pdf_gtor.make_report(rept_type)

  return pdf_file_path
end

# ---

def send_file(subscription, attachments, title)
  return if attachments.empty?
  if ok_to_send(subscription) then
    emailManager = Entity.find_by_uuid("EMailManager")
    fromAddress = (emailManager.nil?) ? '' : emailManager.get_attr_val('fromAddress')
    @fromAddress = (fromAddress.to_s.empty?) ? 'reports@vmturbo.com' : fromAddress

    #Send email:
    filepath = attachments.gsub(' -a /srv/reports/pdf_files/',' ')
    $logger.info "#{Time.now.to_s} Info: Sending report: #{filepath}"
    rstr = `mailx #{attachments} -s "VMTReports: E-Mail Subscriptions (#{title})" -r '#{@fromAddress}' '#{subscription.email.to_s.gsub(',', ' ')}' < script/email_subscription_msg.txt`
  else
    $logger.info "#{Time.now.to_s} Info: Deferred sending until subscription params match"
  end
end

def verify_uncompressed_report_exits(report, file_path, format)
  date_str = report_date(report)
  if !File.exists?(file_path) && File.exists?(file_path+".gz")
    `[ ! -d /tmp/pdf_files ] && mkdir /tmp/pdf_files`
    `[ ! -d /tmp/pdf_files/#{date_str} ] && mkdir /tmp/pdf_files/#{date_str}`
    `[ ! -f /tmp/pdf_files/#{date_str}/#{report.filename.to_s}.#{format.to_s} ] && gzip -dc #{file_path}.gz > /tmp/pdf_files/#{date_str}/#{report.filename.to_s}.#{format.to_s}`
    `chown -R wwwrun /tmp/pdf_files`
    `chgrp -R www /tmp/pdf_files` 

    file_path = "/tmp/pdf_files/#{date_str}/#{report.filename.to_s}.#{format.to_s}"
  end
  return file_path
end

def report_date(rept)
  dates_available = rept.dates_available.reverse
  date_str = dates_available.first
    date_str = Date.today().to_s(:db) unless date_str
    return date_str
end

# --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

begin
  
  $logger = Logger.new("log/reports.log")


  $logger.info "#{Time.now.to_s} Info: Emailing reports to subscribers..."


  if ! environment_checks_pass() then
    $logger.error("#{Time.now.to_s} ERROR: Environment checks failed. Report e-mail sending aborted.")
    exit
  end


  for subscription in ReportSubscription.find(:all) do

    next if subscription.is_obsolete?
    next if ! ok_to_send(subscription)


    $logger.info "#{Time.now.to_s} Info: To:     #{subscription.email} "
    $logger.info "#{Time.now.to_s} Info: Report: #{subscription.get_report.title}"


    # a subscription object has a link to either a standard or a custom report
    # one link will be null and the other will be set

    # rept will be non-null if this is a standard report subscription

    begin
      rept = subscription.standard_report

      if rept then
        # Prepare list of report paths
        paths = REPORT_EXTS.inject({}) do |pts, ext|
          today_date_str = (subscription.period == "Daily") ? Date.today().to_s(:db) : report_date(rept)
          rptPath = "#{ENV['VMT_REPORTS_HOME']}/pdf_files/#{today_date_str}/#{rept.filename}.#{ext}"
          pts[rptPath] = ext 
          pts
        end
        
        # modify paths list, uncompress if necessary
        paths = paths.inject({}) do |newPaths, (path, ext)| 
          newPaths[verify_uncompressed_report_exits(rept, path, ext)] = ext
          newPaths
        end

        attachments = paths.inject('') do |atts, path_ext|
          atts += File.exists?(path_ext[0]) ? " -a #{path_ext[0]} " : ''
        end

        if rept.user_selected? && !attachments.empty? then
          send_file(subscription, attachments, rept.title.to_s)
        end
      end
    rescue => e
      begin
        ReportSubscription.delete(subscription.id)
        $logger.error "Exception (#{e.message}) #{rept.title.to_s} not sent"
        $logger.error e.backtrace.join("\n")
      rescue
      end
    end


    # rept will be non-null if this is a custom report subscription

    begin
      rept = subscription.custom_report
      if rept then
        prepare_custom_report_pdf_file(rept)
        filepath = custom_rept_file(rept)
        if File.exists?(filepath) then
          send_file(subscription,(' -a '+filepath+' '),rept.title.to_s)
        end
      end
    rescue => e
      begin
        ReportSubscription.delete(subscription.id)
        $logger.error "Exception (#{e.message}) #{rept.title.to_s} not sent"
        $logger.error e.backtrace.join("\n")
      rescue
      end
    ensure
      begin
        FileUtils.rm(filepath) unless (filepath.nil? || filepath.empty?)          
      rescue
        $logger.error "Unable to delete custom report pdf file: #{filepath}"
      end
    end


    # rept will be non-null if this is an on-demand report subscription

    begin
      rept = subscription.on_demand_report
      if rept then
        filepath = prepare_on_demand_report_pdf_file(subscription,rept)
        if File.exists?(filepath) then
          send_file(subscription,(' -a '+filepath+' '),rept.title.to_s)
        else
          $logger.error "File missing (#{filepath}) #{rept.title.to_s} not sent"
        end
      end
    rescue => e
      begin
        $logger.error "Exception (#{e.message}) #{rept.title.to_s} not sent"
        $logger.error e.backtrace.join("\n")
      rescue
      end
    end


    $logger.info ""

  end

rescue
end
