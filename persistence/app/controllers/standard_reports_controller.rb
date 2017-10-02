require 'topology_svc'

class StandardReportsController < ApplicationController

  before_filter :login_required

  @@format_suffix_name_map = {'PDF'=>'pdf','pdf'=>'PDF','Excel'=>'xls','xls'=>'Excel', 'xlsx'=>'Excel'}

  def index
    prepare_list()
    respond_to do |format|
      format.html {
        render :action => @ft_custom_action and return if @ft_custom_action
        render :action => 'list'
      }
      format.xml  { render :xml => @standard_reports }
    end
  end

  def apply_to_checked_items
    if params[:action_to_perform] == 'Delete'    
        StandardReport.destroy(params[:delchk].keys) if params[:delchk] && params[:delchk].keys.size > 0
    end 

    redirect_to :back
  end

  def replace_column_value
    if params[:old_value].empty?
        where_clause = "#{params[:field]} is null or #{params[:field]} = ''" 
    elsif params[:old_value].include?("%") || params[:old_value].include?("*")
        where_clause = "#{params[:field]} like '#{params[:old_value].gsub("*","%").strip}'"
    else
        where_clause = "#{params[:field]} = '#{params[:old_value].strip}'"
    end
    StandardReport.update_all("#{params[:field]} = '#{params[:new_value].strip}'", where_clause)
    redirect_to :back
  end

  def delete_matching_rows
    if params[:value].empty?
        where_clause = "#{params[:field]} is null or #{params[:field]} = ''" 
    elsif params[:value].include?("%") || params[:value].include?("*")
        where_clause = "#{params[:field]} like '#{params[:value].gsub("*","%").strip}'"
    else
        where_clause = "#{params[:field]} = '#{params[:value].strip}'"
    end
    StandardReport.delete_all(where_clause)
    redirect_to :back
  end

  def quick_add_items
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      for str_name in names do
        str_name.strip!
        StandardReport.find_or_create_by_filename(str_name)
      end
    end
    redirect_to :action => 'list'
  end

  def list_with_sort
    redirect_to params.merge(:action => 'list')
  end

  def list_without_sort
    params[:sort_field] = params[:sort_field2] = params[:sort_field3] = nil
    redirect_to params.merge(:action => 'list')
  end

  def list_with_filter
    params[:page] = "1"
    redirect_to params.merge(:action => 'list')
  end

  def list_without_filter
    params[:page] = "1"
    params[:search_field] = params[:search_field2] = params[:search_field3] = nil
    params[:search_str] = params[:search_str2] = params[:search_str3] = nil
    redirect_to params.merge(:action => 'list')
  end

  def list_by_page
    redirect_to params.merge(:action => 'list')
  end

  def block_list_by_page
    redirect_to params.merge(:action => 'block_list')
  end

  def list
    prepare_list()
    respond_to do |format|
      format.html {
        render :action => @ft_custom_action if @ft_custom_action
      }
      format.xml  { render :xml => @standard_reports }
    end
  end

  def block_list
    prepare_list()
    render :action => @ft_custom_action if @ft_custom_action
  end

  def new
    @new_obj = @standard_report = StandardReport.new

    @ft_subject = "Standard Report"
    @ft_activity = "New"

    if params && params[:redirect] 
        session[:post_create_redirect] = params[:redirect]
    end

    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @standard_report }
    end
  end

  def set_context(load_related=true)
    @standard_report = StandardReport.find(params[:id])

    @ft_subject = "Standard Report"
    @ft_activity = @standard_report.iname
    
    load_related = false if (params[:action] == "show_properties")    # don't load related items if only showing properties
    return unless load_related

    @order_field = get_order_field(params,'standard_report','report_subscriptions','email')
    @id_set = @standard_report.send("#{'report_subscriptions'.singularize}_ids").concat(["0"]).join(",")
    @report_subscriptions = ReportSubscription.find_by_sql(%Q~select * from #{ReportSubscription.default_query("report_subscription.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

  end

  def show
    set_context()
    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @standard_report }
    end
  end

  def show_properties
    set_context()
    render :partial => 'show_properties'
  end

  def show_related
    set_context()
    render :partial => 'show_related'
  end

  def get_related
    set_context()
    render :xml => { :message => "No such related items" }.to_xml(:root => "error") 
  end

  def show_as_doc
    set_context()
    render :action => @ft_custom_action if @ft_custom_action
  end

  def edit
    set_context()
    render :action => @ft_custom_action if @ft_custom_action
  end

  # returns true to indicate redirection has been queued
  def update_aggregator(new_obj,params)
    return false
  end

  def sanitize_params(hk)
    params[hk].each{|k,v| params[hk][k] = v.gsub("-- select --","")}  if params[hk]
    params[hk].each{|k,v| params[hk][k] = v.gsub(/HH:MM.*/,"")}       if params[hk]
  end

  def create
    sanitize_params(:standard_report)
    @new_obj = @standard_report = StandardReport.new(params[:standard_report])

    if ! ok_to_add_or_update?
        flash[:notice] = 'Not added. ' + @reason.to_s
        @ft_subject = "Standard Report"
        @ft_activity = "New"
        render :action => 'new'
    else    
      respond_to do |format|
        if @standard_report.save
          format.xml  { render :xml => @standard_report, :status => :created, :location => @standard_report }
          format.html {
            flash[:notice] = "#{@standard_report.iname} was successfully created."
            redirected_to_aggregator = update_aggregator(@standard_report,params)
            if ( ! redirected_to_aggregator ) && session[:post_create_redirect]
                url = session[:post_create_redirect] + "?standard_report_id=" + @standard_report.id.to_s + "&standard_report_ids[]=" + @standard_report.id.to_s 
                redirect_to url
                session[:post_create_redirect] = nil
            else 
                redirect_to :id => @standard_report.id, :action => (params[:commit].to_s.include?("Another") ? 'new' : 'edit') if ! redirected_to_aggregator 
            end
          }
        else
          format.xml  { render :xml => @standard_report.errors, :status => :unprocessable_entity }
          format.html {
            flash[:notice] = 'Standard Report could not be created. See errors.'
            render :action => 'new'
          }
        end
      end
    end
  end

  def update
    @updated_obj = @standard_report = StandardReport.find(params[:id])
    sanitize_params(:standard_report)

    if ! ok_to_add_or_update?
      flash[:notice] = 'Not updated. ' + @reason.to_s
      @ft_subject = "Standard Report"
      @ft_activity = "Edit"
      redirect_to :action => 'edit', :id => @standard_report and return
    end 

      respond_to do |format|
      if @standard_report.update_attributes(params[:standard_report])

        if session[:post_update_redirect] then
          redirect_to session[:post_update_redirect]
          session[:post_update_redirect] = nil
          return
        end

        format.xml  { head :ok }
        format.html {
          flash[:notice] = 'Standard Report was successfully updated.'
          redirect_to :action => 'show', :id => @standard_report
        }
      else
        format.xml  { render :xml => @standard_report.errors, :status => :unprocessable_entity }
        format.html { 
          redirect_to :action => 'edit', :id => @standard_report
        }
      end
    end
  end

  def ok_to_add_or_update?
    if params[:standard_report][:filename].to_s.empty?
      @reason = 'The filename field can not be blank.'
      return false
    end
    return true
  end

  def destroy
    obj = StandardReport.find(params[:id])
    obj.destroy
    respond_to do |format|
      format.html {
        redirect_to :back
      }
      format.xml  { head :ok }
    end
  end


  # -------------------------------------------------

  def add_to_report_subscriptions
    @ft_subject = "Standard Report"
    @ft_activity = "Select Report Subscription".pluralize

    @standard_report = StandardReport.find(params[:id])
    @report_subscriptions = ReportSubscription.choices_for(@standard_report,'report_subscriptions')
    @report_subscriptions = @report_subscriptions - [ @standard_report ]
    @my_report_subscriptions = @standard_report.report_subscriptions
    @report_subscriptions.delete_if{|a| @my_report_subscriptions.include?(a)}
  end
  
  def update_report_subscriptions
    @standard_report = StandardReport.find(params[:id])
    @report_subscriptions = [ ReportSubscription.find(params[:report_subscription_ids]) ] if params[:report_subscription_ids]
    @standard_report.report_subscriptions << @report_subscriptions if @report_subscriptions
    redirect_to :controller => @standard_report.ctrlr_name, :action => 'edit', :id => @standard_report, :anchor => 'report_subscriptions'
  end

  def remove_from_report_subscriptions
    @standard_report = StandardReport.find(params[:id])
    @report_subscriptions = ReportSubscription.find(params[:report_subscription_id])
    @standard_report.report_subscriptions.delete @report_subscriptions
    redirect_to :controller => @standard_report.ctrlr_name, :action => 'edit', :id => @standard_report, :anchor => 'report_subscriptions'
  end

  def quick_add_report_subscriptions
    @standard_report = StandardReport.find(params[:id])
    
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      existing_report_subscriptions = @standard_report.report_subscriptions.find(:all)
      for str_name in names do
        str_name.strip!
        new_obj = ReportSubscription.find_or_create_by_email(str_name)
        @standard_report.report_subscriptions << new_obj unless existing_report_subscriptions.include?(new_obj)
      end
    end
    
    redirect_to :action => 'edit', :id => @standard_report.id
  end


  def csv_upload
    # no need to prepare any data - just show the view to post against 'upload_csv'
  end

  def upload_csv

    # commented out - instead try to load the csv without an intermediate file
    #fn = "#{RAILS_ROOT}/uploads/standard_report_data.csv"
    #File.open(fn, "wb") { |f| f.write(params[:csv_file].read) }
    #StandardReport.load_csv_file(fn) if StandardReport.respond_to? :load_csv_file

    StandardReport.load_csv_str(params[:csv_file].read) if StandardReport.respond_to? :load_csv_str
    
    redirect_to :action => 'list' 
  end

  def download_csv
    attr_names = StandardReport.value_attrs

    csv_string = String.new
    csv_string = attr_names.join(",") + "\n"

    # override visual pagination establishing the limit to 100,000 rows
    params[:page] = '1'
    params[:page_length] = '100000'

    prepare_list()

    all_objs = @standard_reports      
    
    if all_objs.size > 0 then
      all_objs.each do |record| 
        csv_string << '"'+attr_names.collect{|s| record.get_attr_val(s,'csv').to_s.gsub("\"","\"\"")}.join('","')+'"' + "\n"
      end
    end
    send_data csv_string, :filename => 'standard_report_data.csv', :type => 'text/csv'
  end 

  # --- 

  def show_queries
    @ft_subject = "StandardReport"
    @ft_activity  = "Reports"

    @queries = StandardReport.query_specs

    render :partial => 'fast_ops/queries', :layout => 'application'
  end

  def run_query
    @def_page_size ||= 20
    @page_size = params[:page_length].to_i > 0 ? params[:page_length].to_i : @def_page_size

    @page_no = [ params[:page].to_i, 1 ].max
    @page_clause = nil # "LIMIT #{@page_size.to_s} OFFSET #{((@page_no-1)*@page_size).to_s}"

    @query = StandardReport.query_specs[params[:query]]
    @sort_fields = params[:order_by] || (! @query[:order_by].to_s.empty? && @query[:order_by].join(",")) || @query[:cols][0]
    @where_clause = params[:where] || (@query[:where].to_s.length > 0 ? @query[:where] : nil)

    @base_sql = @query[:sql] + (@where_clause ? " WHERE #{@where_clause}" : "") 
    @count = StandardReport.count_by_sql("SELECT COUNT(id) FROM (#{@base_sql}) AS row_data")
    
    @sql = @base_sql + (@sort_fields ? " ORDER BY #{@sort_fields}" : "") + (@page_clause ? " #{@page_clause}" : "") 
    @rows = StandardReport.find_by_sql(apply_limit(@sql))

    @cols = @query[:cols] || (@rows.size > 0 && @rows[0].attributes.keys)

    @link_params = { :controller => 'standard_reports', :action => 'show' }

    return @rows
  end

  def render_query_results
    @order_field = params[:order_by]
    params[:order_by] = quoted_name(@order_field) if params[:order_by]
    params[:order_by] += " #{params[:direction]}" if params[:direction]

    run_query()

    params[:order_by] = @order_field
    render :partial => 'fast_ops/query_results'
  end

  def before_prepare_list
    # override to set custom params and vars
  end

  def prepare_list
    orig_params = params.clone

    before_prepare_list()

    @ft_subject  = ( @ft_subject || "Standard Report".pluralize )
    @ft_activity = ( @ft_activity || "All" )

    @order_field = ( params[:order_by] || "filename" )
    @direction = ( params[:direction] || "ASC" )

    @query_param = ( params[:query] || "list_all" )
    params[:query] = @query_param
    
    params[:order_by] = quoted_name(@order_field) + " " + @direction

    @where_param = params[:where]  # save the original where param

    adjust_sorting_and_filtering_params(StandardReport,params)
    @order_field = params[:sort_field].to_s unless params[:sort_field].to_s.empty?

    @standard_reports = run_query()

    # restore original params passed to the server
    params.clear
    params.merge!(orig_params)
  end

  # --- 

#BEGIN-UID.usermethods

  require 'pdf_generators'
  require 'fileutils'


  MakeAllCaps = ['pm','vm','cpu','vcpu']

  def make_title(fname)
    fname = fname.gsub(/daily_|weekly_/, "")
    words = fname[/\w+/].split("_")
    desc = ""
    words[0..-2].each{ |w|
      w = w.capitalize
      w = w.upcase if MakeAllCaps.include?(w.downcase)
      w = w.downcase if w.size < 3 && ! MakeAllCaps.include?(w.downcase)
      desc << w + " "
    }
    return desc.strip
  end

  before_filter :redirect_to_list, :only => [ :list ]
  def redirect_to_list
    session[:post_create_redirect] = url_for(:controller => 'standard_reports', :action => 'list')
    return true
  end

  before_filter :populate_list_of_reports, :only => [ :index ]
  def populate_list_of_reports

    return true   # only used in development - just return in normal use

    for fname in Dir.new("#{ENV['VMT_REPORTS_HOME']}/VmtReports") do
      if fname[".rptdesign"]
        rept = StandardReport.find_or_create_by_filename(fname[/\w+/])

        if rept.title.to_s.empty?
          rept.title = make_title(fname)
        end

        if rept.short_desc.to_s.empty?
          rept.short_desc = make_title(fname)
        end

        if rept.category.to_s.empty?
          rept.category = "Utilization"
        end

        rept.save
      end
    end

    return true
  end

  # # #

  def before_prepare_list
    @def_page_size = 200
    params[:page_length] = 200
  end

  def list
    prepare_list()

    respond_to do |format|
      format.html {
        render :action => @ft_custom_action if @ft_custom_action
      }
      format.xml  { render :xml => @standard_reports }
    end
  end

  # # #

  def set_context(load_related=true)
    @standard_report = StandardReport.find(params[:id])

    @ft_subject = "Standard Report"
    @ft_activity = @standard_report.title

    load_related = false if (params[:action] == "show_properties")    # don't load related items if only showing properties
    return unless load_related

    @order_field = get_order_field(params,'standard_report','report_subscriptions','email')
    @id_set = @standard_report.send("#{'report_subscriptions'.singularize}_ids").concat(["0"]).join(",")
    @report_subscriptions = ReportSubscription.find_by_sql(%Q~select * from #{ReportSubscription.default_query("report_subscription.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

  end

  def get_pdf_file_path
    date_str = params[:date].to_s
    format = params[:format].to_s
  
    if date_str == "MostRecent" || date_str.empty? then
      dates_available = @standard_report.dates_available.reverse
      date_str = dates_available.first
      date_str = Date.today().to_date_image unless date_str
    end

    dir  = "#{ENV['VMT_REPORTS_HOME']}/pdf_files/#{date_str}/"
    file = "#{@standard_report.filename.to_s}"
    ext = format.to_s
    
    if(format=='xls' && (File.exists?(dir+file+'.xlsx') || File.exists?(dir+file+'.xlsx.gz')) )
      ext = 'xlsx'
    end
    
    pdf_file_path = dir+file+'.'+ext 

    if File.exists?(pdf_file_path+".gz")
        `[ ! -d /tmp/pdf_files ] && mkdir /tmp/pdf_files`
        date_dir = "/tmp/pdf_files/#{date_str}"
        `[ ! -d #{date_dir} ] && mkdir #{date_dir}`
        `[ ! -f #{date_dir}/#{file}.#{ext} ] && gzip -dc #{pdf_file_path}.gz > #{date_dir}/#{file}.#{ext}`
        pdf_file_path = "#{date_dir}/#{file}.#{ext}"
    end
      
    return pdf_file_path
  end


  def show
    set_context()

    unless params[:send_email].to_s.empty?
      render :action => 'send_form.html', :layout => false
      return
    end

    pdf_file_path = get_pdf_file_path

    if !File.exists?(pdf_file_path)
      render :inline => "This "+@@format_suffix_name_map[params[:format]]+" report is not currently available : #{pdf_file_path} ",
             :layout => false
      return
    end

    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @standard_report }
      format.pdf  { send_file pdf_file_path, :type=> "application/pdf", :disposition => 'inline' }
      format.xls  { send_file pdf_file_path, :type=> "application/msexcel", :disposition => 'attachment' }
      format.xlsx { send_file pdf_file_path, :type=> "application/msexcel", :disposition => 'attachment' }
    end
  end

  def send_via_email
    set_context()

    valid_email = validate_email_address(params[:email_addresses])
    if valid_email
      pdf_file_path = get_pdf_file_path
      
      if !File.exists?(pdf_file_path)
        render :inline => "This "+@@format_suffix_name_map[params[:format]]+" report is not currently available",
               :layout => false
        return
      end
      
      emailManager = Entity.find_by_uuid("EMailManager")
      fromAddress = (emailManager.nil?) ? '' : emailManager.get_attr_val('fromAddress')
      @fromAddress = (fromAddress.to_s.empty?) ? 'reports@vmturbo.com' : fromAddress
      
      cmd = "mailx -a #{pdf_file_path} -r '#{@fromAddress.to_s}' -s 'VMTurbo: #{@standard_report.title.to_s}' '#{params[:email_addresses].to_s.gsub(/[; ]/,',')}' < /dev/null"
      rc = system cmd
    end
    msg = valid_email ? (rc ? "Message sent " : ("Command execution returned false for: "+cmd)) : "Invalid email address"
    render :inline => msg,
           :layout => false
  end
  
  def validate_email_address(addresses)
  valid = !addresses.nil?
    if valid
      addresses.split(",").each do |address|
        valid &= address.strip =~ /^(|(([A-Za-z0-9]+_+)|([A-Za-z0-9]+\-+)|([A-Za-z0-9]+\.+)|([A-Za-z0-9]+\++))*[A-Za-z0-9]+@((\w+\-+)|(\w+\.))*\w{1,63}\.[a-zA-Z]{2,6})$/
      end
    end
    return valid
  end

  # ---

  def get_se_by_uuid_or_name(params)
    uuid = params[:uuid].to_s
    
    unless uuid.empty?
      se = Entity.find_by_uuid(uuid)
    else
      class_name = params[:class_name].to_s
      machine_name = params[:machine_name].to_s.gsub(/'+/,"''")
      se = Entity.find(:first, :conditions => "creation_class = '#{class_name}' and display_name = '#{machine_name}'")
    end

    return se
  end

  def show_se_on_demand
    @se = get_se_by_uuid_or_name(params)
  
    customer_name = params[:customer_name];
    if customer_name.to_s.empty?
      @customer_name='VMTurbo'
    else
      @customer_name=customer_name
    end
    #puts "customer_name: " +customer_name
    
    top_svc = TopologyService.new

#    puts current_user.inspect
#    puts current_user[:remember_token].inspect

    obj_xml = top_svc.xml_for(current_user[:remember_token],"userType")
    topologyElement = top_svc.attrs_for(obj_xml)
    userType = topologyElement.get_attribute("userType").value
    
    shared_customer = false
    if(userType == "SharedCustomer")
      shared_customer = true
    end

    if @se
      @class_name = @se.creation_class.to_s
      @machine_name = @se.display_name.to_s.gsub("'","''")

      unless params[:send_email].to_s.empty?
        render :action => 'send_form_se.html', :layout => false
        return
      end

    pdf_gtor = PdfGeneratorForSvcEntity.new(@machine_name, @class_name, @se.uuid, "profile", @customer_name, params[:format] , params[:num_days_ago] , params[:show_charts], shared_customer)
                                                     
      unless pdf_gtor.gen_file_exists? || params[:wait_for_gen].to_s == 'true'
        @for_item = @se.display_name
        @redirect_url = url_for( params.merge({:wait_for_gen => 'true'}) )
        render :action => 'wait_for_gen.html', :layout => false
        return
      end

      pdf_file_path = pdf_gtor.make_report()
      if !File.exists?(pdf_file_path)
        render :inline => "This "+@@format_suffix_name_map[params[:format]]+" report is not currently available",
               :layout => false
        return
      end

      respond_to do |format|
        format.pdf  { send_file pdf_file_path, :type=> "application/pdf", :disposition => 'inline' }
        format.xls  { send_file pdf_file_path, :type=> "application/msexcel", :disposition => 'attachment' }
        format.xlsx { send_file pdf_file_path, :type=> "application/msexcel", :disposition => 'attachment' }
      end

    else
      render :inline => "Data not available for this selection #{params[:machine_name].to_s}",
             :layout => false
    end
  end

  def send_se_via_email
    @se = get_se_by_uuid_or_name(params)

    valid_email = validate_email_address(params[:email_addresses])
  
    if @se && valid_email
      @class_name = @se.creation_class.to_s
      @machine_name = @se.display_name.to_s.gsub("'","''")
    
      pdf_gtor = PdfGeneratorForSvcEntity.new(@machine_name, @class_name, @se.uuid)
      pdf_file_path = pdf_gtor.make_report()

      if !File.exists?(pdf_file_path)
        render :inline => "This "+@@format_suffix_name_map[params[:format]]+" report is not currently available",
               :layout => false
        return
      end
    
      emailManager = Entity.find_by_uuid("EMailManager")
      fromAddress = (emailManager.nil?) ? '' : emailManager.get_attr_val('fromAddress')
      @fromAddress = (fromAddress.to_s.empty?) ? 'reports@vmturbo.com' : fromAddress
         
      cmd = "mailx -a #{pdf_file_path} -r '#{@fromAddress}' -s 'VMTurbo: #{@machine_name.gsub(/[^0-9a-zA-Z_ ]/,'')}' '#{params[:email_addresses].to_s.gsub(/[; ]/,',')}' < /dev/null"
      rc = system cmd
      render :inline => rc ? "Message sent" : ("Command execution returned false for: "+cmd),
             :layout => false
    else
      msg = valid_email ? "Data not available for this selection #{params[:machine_name].to_s}" : "Invalid email address"
      render :inline => msg,
             :layout => false
    end
  end

  # ---

  def get_se_group_by_uuid_or_name(params)
    uuid = params[:uuid].to_s

    unless uuid.empty?
      se = Entity.find_by_uuid(uuid)
    else
      class_name = "Group"
      group_name = params[:group_name].to_s.gsub(/'+/,"''").gsub(/\\+/,"\\\\\\\\")
      se = Entity.find(:first, :conditions => "creation_class = '#{class_name}' and display_name = '#{group_name}'")
    end

    return se
  end

  def show_se_group_on_demand
       
    @group = get_se_group_by_uuid_or_name(params)
    if @group
      customer_name = params[:customer_name]
    if customer_name.to_s.empty?
    @customer_name='VMTurbo'
    else
    @customer_name=customer_name
    end
    # puts params.inspect
    
      @group_name = @group.display_name.to_s.gsub("'","''").gsub("\\","\\\\\\\\")
      @class_name = @group.get_attr_val('SETypeName')
      @rept_type = params[:rept_type] || "profile"
      
      unless params[:send_email].to_s.empty?
        render :action => 'send_form_se_group.html', :layout => false
        return
      end

    top_svc = TopologyService.new

#    puts current_user.inspect
#    puts current_user[:remember_token].inspect

    obj_xml = top_svc.xml_for(current_user[:remember_token],"userType")
    topologyElement = top_svc.attrs_for(obj_xml)
    userType = topologyElement.get_attribute("userType").value
    
    shared_customer = false
    if(userType == "SharedCustomer")
      shared_customer = true
    end

    pdf_gtor = PdfGeneratorForGroup.new(@group_name.gsub("\\","\\\\\\\\"), @class_name, @group.uuid, @rept_type, @customer_name, params[:format] , params[:num_days_ago], false, shared_customer)

      unless pdf_gtor.gen_file_exists? || params[:wait_for_gen].to_s == 'true'
        @for_item = @group.display_name
        @redirect_url = url_for( params.merge({:wait_for_gen => 'true'}) )
        render :action => 'wait_for_gen.html', :layout => false
        return
      end

      pdf_file_path = pdf_gtor.make_report(@rept_type)

      if !File.exists?(pdf_file_path)
        render :inline => "This "+@@format_suffix_name_map[params[:format]]+" report is not currently available",
               :layout => false
        return
      end 
      
      respond_to do |format|
        format.xls  { send_file pdf_file_path, :type=> "application/msexcel", :disposition => 'attachment' }
        format.xlsx { send_file pdf_file_path, :type=> "application/msexcel", :disposition => 'attachment' }
        format.pdf  { send_file pdf_file_path, :type=> "application/pdf", :disposition => 'inline' }
      end

    else
      render :inline => "Data not available for this selection #{params[:group_name].to_s}",
             :layout => false
    end

  end

  def send_se_group_via_email
    @group = get_se_group_by_uuid_or_name(params)
  
    valid_email = validate_email_address(params[:email_addresses])

    if @group && valid_email
      @group_name = @group.display_name.to_s.gsub("'","''").gsub("\\","\\\\\\\\")
      @class_name = @group.get_attr_val('SETypeName')
      @rept_type = params[:rept_type] || "profile"

      pdf_gtor = PdfGeneratorForGroup.new(@group_name.gsub("\\","\\\\\\\\"), @class_name, @group.uuid)
      pdf_file_path = pdf_gtor.make_report(@rept_type)

      if !File.exists?(pdf_file_path)
        render :inline => "This "+@@format_suffix_name_map[params[:format]]+" report is not currently available",
               :layout => false
        return
      end
    
      emailManager = Entity.find_by_uuid("EMailManager")
      fromAddress = (emailManager.nil?) ? '' : emailManager.get_attr_val('fromAddress')
      @fromAddress = (fromAddress.to_s.empty?) ? 'reports@vmturbo.com' : fromAddress
         
      cmd = "mailx -a #{pdf_file_path} -r '#{@fromAddress}' -s 'VMTurbo: #{@group_name.gsub(/[^0-9a-zA-Z_ ]/,'')}' '#{params[:email_addresses].to_s.gsub(/[; ]/,',')}' < /dev/null"
      rc = system cmd
      render :inline => rc ? "Message sent" : ("Command execution returned false for: "+cmd),
             :layout => false

    else
       msg = valid_email ? "Data not available for this selection #{params[:machine_name].to_s}" : "Invalid email address"
      render :inline => msg,
             :layout => false
    end
  end

  # ---

  def get_items(look_for,class_name)
    @items = Entity.find(:all, :conditions => ["creation_class = ?", class_name])
    @items = @items.collect{|x| x.display_name.to_s}.compact
    begin
      @items = @items.select{|x| x.downcase[/#{look_for}/]}
    rescue
      @items = @items.select{|x| x.downcase[look_for]}
    end
    @items = @items[0,24] || []
    @items.sort!

    render :inline => "<%= content_tag(\"ul\", @items.map { |entry| content_tag(\"li\", entry) }.uniq.join(\"\")) %>"
  end

  def auto_complete_for_vms_vmname
    look_for = params[:vms][:vmname].downcase
    get_items(look_for,"VirtualMachine")
  end
  
  def auto_complete_for_pms_pmname
    look_for = params[:pms][:pmname].downcase
    get_items(look_for,"PhysicalMachine")
  end

  def auto_complete_for_sds_sdname
    look_for = params[:sds][:sdname].downcase
    get_items(look_for,"Storage")
  end

  def get_group_items(look_for, class_name, filter=nil)
    baseQuery = "select internal_name, group_name from entity_groups where group_type = ?"
  	query = filter.nil? ? [baseQuery, class_name] : [baseQuery+" and internal_name like ?", class_name, filter]
  	
    @items = StandardReport.find_by_sql(query)
    @items = @items.reject{|r| r.internal_name[/-vms$/]}

    @items = @items.collect{|r| r.group_name}.reject { |n| n.to_s.empty? || n.to_s[","] || n.to_s["ExcludeFrom"] }

    begin
      @items = @items.select{|x| x.downcase[/#{look_for}/]}
    rescue
      @items = @items.select{|x| x.downcase[look_for]}
    end
    @items = @items[0,24] || []
    @items.sort!

    render :inline => "<%= content_tag(\"ul\", @items.map { |entry| content_tag(\"li\", entry) }.uniq.join(\"\")) %>"
  end

  def auto_complete_for_vm_groups_group_name
    look_for = params[:vm_groups][:group_name].downcase
    get_group_items(look_for,"VirtualMachine")
  end
  
  def auto_complete_for_vm_groups_phy_res_group_name
    look_for = params[:vm_groups_phy_res][:group_name].downcase
    get_group_items(look_for,"VirtualMachine")
  end

  def auto_complete_for_vm_groups_over_under_group_name
    look_for = params[:vm_groups_over_under][:group_name].downcase
    get_group_items(look_for,"VirtualMachine")
  end
  
  def auto_complete_for_vm_groups_individual_vm_summary_group_name
    look_for = params[:vm_groups_individual_vm_summary][:group_name].downcase
    get_group_items(look_for,"VirtualMachine")
  end
  
  def auto_complete_for_vm_groups_30_days_vm_top_bottom_capacity_grid_group_name
    look_for = params[:vm_groups_30_days_vm_top_bottom_capacity_grid][:group_name].downcase
    get_group_items(look_for,"VirtualMachine")
  end
  
  def auto_complete_for_ds_groups_group_name
    look_for = params[:ds_groups][:group_name].downcase
    get_group_items(look_for,"Storage")
  end

  def auto_complete_for_pm_groups_group_name
    look_for = params[:pm_groups][:group_name].downcase
    get_group_items(look_for,"PhysicalMachine")
  end

  def auto_complete_for_pm_hosting_groups_group_name
    look_for = params[:pm_hosting_groups][:group_name].downcase
    get_group_items(look_for,"PhysicalMachine")
  end

  def auto_complete_for_pm_top_bottom_groups_group_name
    look_for = params[:pm_top_bottom_groups][:group_name].downcase
    get_group_items(look_for,"PhysicalMachine")
  end
                                                 
  # Filter only clusters:
  def auto_complete_for_pm_cluster_groups_group_name
    look_for = params[:pm_cluster_groups][:group_name].downcase
    get_group_items(look_for,"PhysicalMachine")
  end
  
  # Filter only clusters:
  def auto_complete_for_pm_cluster_heatmap_groups_group_name
    look_for = params[:pm_cluster_heatmap_groups][:group_name].downcase
    grps = get_group_items(look_for,"PhysicalMachine", "GROUP-PMsByCluster%")
  end


  def auto_complete_for_pm_cap_groups_group_name
    look_for = params[:pm_cap_groups][:group_name].downcase

    if ActiveRecord::Base.connection.table_exists?("capacity_projection") then
      @items = StandardReport.find_by_sql("select distinct group_name from capacity_projection order by group_name")
      @items = @items.collect{|r| r.group_name}.select{|n| ! n.to_s.empty?}
      begin
        @items = @items.select{|x| x.downcase[/#{look_for}/]}
      rescue
        @items = @items.select{|x| x.downcase[look_for]}
      end
      @items = @items[0,24] || []

      render :inline => "<%= content_tag(\"ul\", @items.map { |entry| content_tag(\"li\", entry) }.uniq.join(\"\")) %>"
    else
      @items = ["Projection data not yet available"]
      render :inline => "<%= content_tag(\"ul\", @items.map { |entry| content_tag(\"li\", entry) }.uniq.join(\"\")) %>"
    end
  end

  # # #

  def clear_cache
    begin
      dir_path = "#{ENV['VMT_REPORTS_HOME']}/pdf_files/on_demand"
      if File.exists?(dir_path) then
        Dir.new(dir_path).each { |entry|
          next unless entry.to_s.ends_with?(".pdf")

          file_path = "#{dir_path}/#{entry}"
          File.delete(file_path)
        }
      end
    rescue
    end

    redirect_to :action => 'list'
  end


  # # #

  def populate_planner_xml_data(plan_name)
    plan_name = CGI::escape(plan_name.clone)

    cmds = [
      {:svc => "ActionLogService", :method => "getSummaryData",
                  :args => ["#{plan_name}"],
                  :out_file => "Plan_SummaryData.xml" },

      {:svc => "ServiceEntityService", :method => "getMarketComparison",
                  :args => ["PhysicalMachine",".*","priceIndex|state",
                            "Mem_utilization|CPU_utilization|NetThroughput_utilization|IOThroughtput_utilization","#{plan_name}",
                            "priceIndex|TOP","-1","true"],
                  :out_file => "Plan_MarketComparison.xml"},

      {:svc => "ServiceEntityService", :method => "getByPlan",
                  :args => ["PhysicalMachine","PLAN_#{plan_name}","priceIndex|Produces|ProducesSize",".*utilization","Market_C0"],
                  :out_file => "Plan_ByPlanPhysicalMachine.xml"}, 

      {:svc => "ServiceEntityService", :method => "getByPlan",
                  :args => ["Storage","PLAN_#{plan_name}","priceIndex|Produces|ProducesSize",".*utilization","Market_C0"],
                  :out_file => "Plan_ByPlanStorage.xml"},

      {:svc => "ServiceEntityService", :method => "getByExpr",
                  :args => ["PhysicalMachine",".*","priceIndex|Produces|ProducesSize",".*utilization","#{plan_name}"],
                  :out_file => "Plan_ByExprPhysicalMachine.xml"},

      {:svc => "ServiceEntityService", :method => "getByExpr",
                  :args => ["Storage",".*","priceIndex|Produces|ProducesSize",".*utilization","#{plan_name}"],
                  :out_file => "Plan_ByExprStorage.xml"},

      {:svc => "ActionLogService", :method => "get",
                  :args => ["#{plan_name}","-1","true","false","ALL"],
                  :out_file => "Plan_MarketActionLog.xml"}, 
    ]

#
# e.g.
# curl -u guest:guest "localhost/vmturbo/api?inv.c&ActionLogService&get&Market_ActionLog_Market_C0_Balance&-1&true"
# curl -u guest:guest "localhost:8400/vmturbo/api?inv.c&ActionLogService&get&Market_ActionLog_Market_C0_Balance&-1&true"
#

    service_location  = "localhost"
    resource          = "/vmturbo/api?inv.c"
    out_dir           = "#{ENV['VMT_REPORTS_HOME']}/planner_data"

    FileUtils.mkdir_p(out_dir) unless File.exists?(out_dir)

    errors = []
    #puts "\nFetching XML data:"
    for cmd in cmds do
      href = [resource, cmd[:svc], cmd[:method], cmd[:args]].flatten.join("&")
      #puts href
      port = Config::CONFIG['host_os'].to_s["darwin"] ? 8400 : 8080

      Net::HTTP.start(service_location, port) {|http|
        req = Net::HTTP::Get.new(href)
        req.basic_auth 'guest', 'guest'
        response = http.request(req)
        resp = response.body

        filepath = out_dir + '/'+ cmd[:out_file]
        File.open(filepath, 'w'){|f| f.write(resp)}

        errors << "Error: Invalid response; data for #{plan_name} may no longer be available" unless resp.to_s!="" #resp.to_s[/<\w+>/]
      }
    end

    return errors
  end


  #
  # Invoke like this:
  #
  #   http://localhost:port/persistence/standard_reports/get_planner_report/1.pdf?plan_name=plan1
  #

  def get_planner_report
    begin
      @plan_name =  params[:plan_name].to_s
      @plan_display_name = params[:plan_display_name].to_s
      customer_name = params[:customer_name];
      if customer_name.to_s.empty?
        @customer_name='VMTurbo'
      else
        @customer_name=customer_name
      end
      #puts "customer_name: " +customer_name

      pdf_gtor = PdfGeneratorForPlanner.new(@plan_display_name,"Planner",nil,"summary", @customer_name)

      unless params[:wait_for_gen].to_s == 'true'
        # don't reuse the old file if re-running the planner report
        File.delete(pdf_gtor.gen_file_path) if File.exists?(pdf_gtor.gen_file_path)

        @for_item = @plan_display_name
        @redirect_url = url_for( params.merge({:wait_for_gen => 'true'}) )
        render :action => 'wait_for_gen.html', :layout => false
        return
      end

      errors = populate_planner_xml_data(@plan_name)
      
      if errors.empty?
        pdf_file_path = pdf_gtor.make_report("summary")

        if params[:format] == "pdf" && ! File.exists?(pdf_file_path)
          render :inline => "This PDF report is not currently available",
                 :layout => false
          return
        end

        respond_to do |format|
          format.html { render :inline => "This resource is designed to produce only a PDF" }
          format.pdf  { send_file pdf_file_path, :type=> "application/pdf", :disposition => 'inline' }
        end
      else
        logger.error errors.join("\n")

        render :inline => "This PDF report is not currently available",
               :layout => false
      end

    rescue => e

      logger.error e.message
      logger.error e.backtrace.to_a[0,12].join("\n")

      render :inline => "An error occurred while creating the planner report",
             :layout => false

    end
  end


  # # #

  def log_file
    respond_to do |format|
      log_file_path = "#{ENV['persistence_BASE']}/log/reports.log"
      format.html { send_file log_file_path, :type=> "text/plain", :disposition => 'inline' }
    end
  end

  def refresh
    set_context()

    cmd = "bash #{ENV['VMT_REPORTS_HOME']}/bin/make_pdf.sh #{@standard_report.filename.to_s}"
    system(cmd)

    flash[:notice] = "The report (#{@standard_report.title.to_s}) has been refreshed"
    redirect_to :action => 'show', :id => @standard_report
  end

  # # #

  def index
    prepare_list()
    respond_to do |format|
      format.html {
        render :action => @ft_custom_action and return if @ft_custom_action
        render :action => 'list'
      }
      # use request param to enable dates to be suppressed
      format.xml  { render :xml => @standard_reports.to_xml(:no_dates => (params[:no_dates].to_s == 'true')) }
    end
  end


#END-UID.usermethods

end