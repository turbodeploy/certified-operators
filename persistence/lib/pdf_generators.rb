require 'fileutils'

class PdfGenerator

  SECS_PER_HOUR = 3600
  EXPIRATION    = 12*SECS_PER_HOUR
 
  attr_accessor :name, :class_name
  
  def initialize(name,class_name,uuid=nil,rept_type="profile", customer_name="VMTurbo", out_format="pdf" , num_days_ago=nil , show_charts=true, shared_customer=false)
    @instance_name, @class_name, @uuid = name, class_name, uuid.to_s
    @rept_type = rept_type
    @customer_name = customer_name
    @out_format = out_format.downcase
    @num_days_ago = num_days_ago
    @show_charts = show_charts
    @shared_customer = shared_customer
  end

  def make_report(rept_type="profile")
    @rept_type = rept_type
    
    pdf_file_path = gen_file_path()
    return pdf_file_path if File.exists?(pdf_file_path) && ( File.mtime(pdf_file_path)+EXPIRATION > Time.now )
      

    #puts "generating report based on new command:"
    @vm_group_name = ("VMs_"+@instance_name).gsub('VMs_PMs_','VMs_').gsub('\\\\\\\\','\\\\')
    gen_rept_cmd = %Q~bash $BIRT_HOME/ReportEngine/genReportRender.sh --format #{@out_format} --output "#{pdf_file_path}" -p "selected_item_uuid=#{@uuid}" -p "pm_name=#{@instance_name}" -p "vm_group_name=#{@vm_group_name}" -p "selected_item_name=#{@instance_name}" -p "plan_name=#{@instance_name}" -p "customer_name=#{@customer_name}" -p "num_days_ago=#{@num_days_ago}" -p "show_charts=#{@show_charts}" -p "shared_customer=#{@shared_customer}" "#{tem_file_path()}"~
    #puts "running report using command:\n"+gen_rept_cmd.to_s
    
    system gen_rept_cmd
    f = File.new(pdf_file_path); f.chmod 0766; f.close()
    
    return pdf_file_path
  end

  def gen_file_exists?
    path = gen_file_path()
    return File.exists?(path)
  end

  def gen_file_path()
    days_ago = @num_days_ago.nil? ? "" : "-"+@num_days_ago.to_s+"_days"
    show_charts_option = @show_charts.nil? ? "" : "-"+@show_charts.to_s
    shared_customer_option = @shared_customer.nil? ? "" : "-"+@shared_customer.to_s
      
    FileUtils.mkdir "#{ENV['VMT_REPORTS_HOME']}/pdf_files/on_demand" unless File.exists?("#{ENV['VMT_REPORTS_HOME']}/pdf_files/on_demand")
    return "#{ENV['VMT_REPORTS_HOME']}/pdf_files/on_demand/" + "#{@class_name}-#{@instance_name}-#{@uuid}-#{@rept_type}#{days_ago}#{show_charts_option}#{shared_customer_option}.#{@out_format}".gsub(/[^0-9a-zA-Z._\-]/,'')
  end
end

# ---

class PdfGeneratorForSvcEntity < PdfGenerator
  def tem_file_path()
    case @class_name
      when "PhysicalMachine"
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/pm_#{@rept_type}.rptdesign"
      when "VirtualMachine"
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/vm_#{@rept_type}.rptdesign"
      when "Storage"
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/sd_#{@rept_type}.rptdesign"
      else
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/other_#{@rept_type}.rptdesign"
    end
  end
end

# ---

class PdfGeneratorForGroup < PdfGenerator
  def tem_file_path()
    case @class_name
      when "PhysicalMachine"
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/pm_group_#{@rept_type}.rptdesign"
      when "VirtualMachine"
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/vm_group_#{@rept_type}.rptdesign"
      when "Storage"
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/sd_group_#{@rept_type}.rptdesign"
      else
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/other_group_#{@rept_type}.rptdesign"
    end
  end
end

# ---

class PdfGeneratorForPlanner < PdfGenerator
  def tem_file_path()
    case @class_name
      when "Planner"
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/planner_#{@rept_type}.rptdesign"
      else
        return "#{ENV['VMT_REPORTS_HOME']}/VmtReportTemplates/other_#{@rept_type}.rptdesign"
    end
  end
end
