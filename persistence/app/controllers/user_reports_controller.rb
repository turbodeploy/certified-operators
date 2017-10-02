class UserReportsController < ApplicationController

  before_filter :login_required

  def index
    prepare_list()
    respond_to do |format|
      format.html {
        render :action => @ft_custom_action and return if @ft_custom_action
        render :action => 'list'
      }
      format.xml  { render :xml => @user_reports }
    end
  end

  def apply_to_checked_items
    if params[:action_to_perform] == 'Delete'    
        UserReport.destroy(params[:delchk].keys) if params[:delchk] && params[:delchk].keys.size > 0
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
    UserReport.update_all("#{params[:field]} = '#{params[:new_value].strip}'", where_clause)
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
    UserReport.delete_all(where_clause)
    redirect_to :back
  end

  def quick_add_items
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      for str_name in names do
        str_name.strip!
        UserReport.find_or_create_by_title(str_name)
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
      format.xml  { render :xml => @user_reports }
    end
  end

  def block_list
    prepare_list()
    render :action => @ft_custom_action if @ft_custom_action
  end

  def new
    @new_obj = @user_report = UserReport.new

    @ft_subject = "User Report"
    @ft_activity = "New"

    if params && params[:redirect] 
        session[:post_create_redirect] = params[:redirect]
    end

    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @user_report }
    end
  end

  def set_context(load_related=true)
    @user_report = UserReport.find(params[:id])

    @ft_subject = "User Report"
    @ft_activity = @user_report.iname
    
    #load_related = false if (params[:action] == "show_properties")    # don't load related items if only showing properties
    #return unless load_related

    #@order_field = get_order_field(params,'user_report','report_subscriptions','email')
    #@id_set = @user_report.send("#{'report_subscriptions'.singularize}_ids").concat(["0"]).join(",")
    #@report_subscriptions = ReportSubscription.find_by_sql(%Q~select * from #{ReportSubscription.default_query("report_subscription.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

  end

  def show
    set_context()
    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @user_report }
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
    sanitize_params(:user_report)
    @new_obj = @user_report = UserReport.new(params[:user_report])

    if ! ok_to_add_or_update?
        flash[:notice] = 'Not added. ' + @reason.to_s
        @ft_subject = "User Report"
        @ft_activity = "New"
        render :action => 'new'
    else    
      respond_to do |format|
        if @user_report.save
          format.xml  { render :xml => @user_report, :status => :created, :location => @user_report }
          format.html {
            flash[:notice] = "#{@user_report.iname} was successfully created."
            redirected_to_aggregator = update_aggregator(@user_report,params)
            if ( ! redirected_to_aggregator ) && session[:post_create_redirect]
                url = session[:post_create_redirect] + "?user_report_id=" + @user_report.id.to_s + "&user_report_ids[]=" + @user_report.id.to_s 
                redirect_to url
                session[:post_create_redirect] = nil
            else 
                redirect_to :id => @user_report.id, :action => (params[:commit].to_s.include?("Another") ? 'new' : 'edit') if ! redirected_to_aggregator 
            end
          }
        else
          format.xml  { render :xml => @user_report.errors, :status => :unprocessable_entity }
          format.html {
            flash[:notice] = 'User Report could not be created. See errors.'
            render :action => 'new'
          }
        end
      end
    end
  end

  def update
    @updated_obj = @user_report = UserReport.find(params[:id])
    sanitize_params(:user_report)

    if ! ok_to_add_or_update?
      flash[:notice] = 'Not updated. ' + @reason.to_s
      @ft_subject = "User Report"
      @ft_activity = "Edit"
      redirect_to :action => 'edit', :id => @user_report and return
    end 

      respond_to do |format|
      if @user_report.update_attributes(params[:user_report])

        if session[:post_update_redirect] then
          redirect_to session[:post_update_redirect]
          session[:post_update_redirect] = nil
          return
        end

        format.xml  { head :ok }
        format.html {
          flash[:notice] = 'User Report was successfully updated.'
          redirect_to :action => 'show', :id => @user_report
        }
      else
        format.xml  { render :xml => @user_report.errors, :status => :unprocessable_entity }
        format.html { 
          redirect_to :action => 'edit', :id => @user_report
        }
      end
    end
  end

  def ok_to_add_or_update?
    if params[:user_report][:title].to_s.empty?
      @reason = 'The title field can not be blank.'
      return false
    end
    return true
  end

  def destroy
    obj = UserReport.find(params[:id])
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
    @ft_subject = "User Report"
    @ft_activity = "Select Report Subscription".pluralize

    @user_report = UserReport.find(params[:id])
    @report_subscriptions = ReportSubscription.choices_for(@user_report,'report_subscriptions')
    @report_subscriptions = @report_subscriptions - [ @user_report ]
    @my_report_subscriptions = @user_report.report_subscriptions
    @report_subscriptions.delete_if{|a| @my_report_subscriptions.include?(a)}
  end
  
  def update_report_subscriptions
    @user_report = UserReport.find(params[:id])
    @report_subscriptions = [ ReportSubscription.find(params[:report_subscription_ids]) ] if params[:report_subscription_ids]
    @user_report.report_subscriptions << @report_subscriptions if @report_subscriptions
    redirect_to :controller => @user_report.ctrlr_name, :action => 'edit', :id => @user_report, :anchor => 'report_subscriptions'
  end

  def remove_from_report_subscriptions
    @user_report = UserReport.find(params[:id])
    @report_subscriptions = ReportSubscription.find(params[:report_subscription_id])
    @user_report.report_subscriptions.delete @report_subscriptions
    redirect_to :controller => @user_report.ctrlr_name, :action => 'edit', :id => @user_report, :anchor => 'report_subscriptions'
  end

  def quick_add_report_subscriptions
    @user_report = UserReport.find(params[:id])
    
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      existing_report_subscriptions = @user_report.report_subscriptions.find(:all)
      for str_name in names do
        str_name.strip!
        new_obj = ReportSubscription.find_or_create_by_email(str_name)
        @user_report.report_subscriptions << new_obj unless existing_report_subscriptions.include?(new_obj)
      end
    end
    
    redirect_to :action => 'edit', :id => @user_report.id
  end


  def csv_upload
    # no need to prepare any data - just show the view to post against 'upload_csv'
  end

  def upload_csv

    # commented out - instead try to load the csv without an intermediate file
    #fn = "#{RAILS_ROOT}/uploads/user_report_data.csv"
    #File.open(fn, "wb") { |f| f.write(params[:csv_file].read) }
    #UserReport.load_csv_file(fn) if UserReport.respond_to? :load_csv_file

    UserReport.load_csv_str(params[:csv_file].read) if UserReport.respond_to? :load_csv_str
    
    redirect_to :action => 'list' 
  end

  def download_csv
    attr_names = UserReport.value_attrs

    csv_string = String.new
    csv_string = attr_names.join(",") + "\n"

    # override visual pagination establishing the limit to 100,000 rows
    params[:page] = '1'
    params[:page_length] = '100000'

    prepare_list()

    all_objs = @user_reports      
    
    if all_objs.size > 0 then
      all_objs.each do |record| 
        csv_string << '"'+attr_names.collect{|s| record.get_attr_val(s,'csv').to_s.gsub("\"","\"\"")}.join('","')+'"' + "\n"
      end
    end
    send_data csv_string, :filename => 'user_report_data.csv', :type => 'text/csv'
  end 

  # --- 

  def show_queries
    @ft_subject = "UserReport"
    @ft_activity  = "Reports"

    @queries = UserReport.query_specs

    render :partial => 'fast_ops/queries', :layout => 'application'
  end

  def run_query
    @def_page_size ||= 20
    @page_size = params[:page_length].to_i > 0 ? params[:page_length].to_i : @def_page_size

    @page_no = [ params[:page].to_i, 1 ].max
    @page_clause = nil # "LIMIT #{@page_size.to_s} OFFSET #{((@page_no-1)*@page_size).to_s}"

    @query = UserReport.query_specs[params[:query]]
    @sort_fields = params[:order_by] || (! @query[:order_by].to_s.empty? && @query[:order_by].join(",")) || @query[:cols][0]
    @where_clause = params[:where] || (@query[:where].to_s.length > 0 ? @query[:where] : nil)

    @base_sql = @query[:sql] + (@where_clause ? " WHERE #{@where_clause}" : "") 
    @count = UserReport.count_by_sql("SELECT COUNT(id) FROM (#{@base_sql}) AS row_data")
    
    @sql = @base_sql + (@sort_fields ? " ORDER BY #{@sort_fields}" : "") + (@page_clause ? " #{@page_clause}" : "") 
    @rows = UserReport.find_by_sql(apply_limit(@sql))

    @cols = @query[:cols] || (@rows.size > 0 && @rows[0].attributes.keys)

    @link_params = { :controller => 'user_reports', :action => 'show' }

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

    @ft_subject  = ( @ft_subject || "User Report".pluralize )
    @ft_activity = ( @ft_activity || "All" )

    @order_field = ( params[:order_by] || "title" )
    @direction = ( params[:direction] || "ASC" )

    @query_param = ( params[:query] || "list_all" )
    params[:query] = @query_param
    
    params[:order_by] = quoted_name(@order_field) + " " + @direction

    @where_param = params[:where]  # save the original where param

    adjust_sorting_and_filtering_params(UserReport,params)
    @order_field = params[:sort_field].to_s unless params[:sort_field].to_s.empty?

    @user_reports = run_query()

    # restore original params passed to the server
    params.clear
    params.merge!(orig_params)
  end

  # --- 

#BEGIN-UID.usermethods

  require 'sql_structs'


  before_filter :redirect_to_list, :only => [ :list ]
  def redirect_to_list
    session[:post_create_redirect] = url_for(:controller => 'user_reports', :action => 'list')
  end


  def before_prepare_list
    @def_page_size = 200
    params[:page_length] = 200
  end

  def get_xml_descriptor
    return CGI::unescape(request.raw_post)[/<\?xml version(.|\n)*<\/user_report>/]
  end

  ### -------------------------------------------

  def send_via_email_form
    set_context()

    render :action => 'send_form.html', :layout => false
    return
  end

  def make_pdf()
    FileUtils.mkdir_p "/tmp"

    html_file = "/tmp/#{@user_report.title.to_s.gsub(/\W/,'')}.html"
    #pdf_file  = "/tmp/#{@user_report.title.to_s.gsub(/\W/,'')}.pdf"
    pdf_file  = "/srv/reports/pdf_files/on_demand/#{@user_report.title.to_s.gsub(/\W/,'')}.pdf"

    html_text = render_to_string :action => 'rows_as_html', :layout => 'basic'
    html_text = html_text.gsub(/\?\d+/,'')

    # fixup refs to resources
    html_text = html_text.gsub(/\/persistence\/(images|stylesheets)\//,'')
    html_text = html_text.gsub('vmt_thinline','vmt_thinline_tmp')

    File.open(html_file,"w") {|f| f.write(html_text)}

    # copy needed css and image files to tmp
    FileUtils.cp "#{ENV['persistence_BASE']}/public/images/logo.jpg", "/tmp/logo.jpg"
    FileUtils.cp "#{ENV['persistence_BASE']}/public/images/vslice_vmt_pod_banner.jpg", "/tmp/vslice_vmt_pod_banner.jpg"
    FileUtils.cp "#{ENV['persistence_BASE']}/public/stylesheets/styles.css", "/tmp/styles.css"

    cmd = "wkhtmltopdf -q #{html_file} #{pdf_file}"
    system(cmd)
    #ret = `#{cmd}`
    #puts ret if !(ret.nil? || ret.empty?)
  
  #Delete the temporary html file:
    begin
      FileUtils.rm(html_file)
    rescue
      logger.error "Unable to delete custom report html file: #{html_file}"
    end
    return pdf_file
  end

  def send_via_email
    prepare_results()

    pdf_file_path = make_pdf()
    
    valid_email= !params[:email_addresses].nil? && params[:email_addresses].match(/^(|(([A-Za-z0-9]+_+)|([A-Za-z0-9]+\-+)|([A-Za-z0-9]+\.+)|([A-Za-z0-9]+\++))*[A-Za-z0-9]+@((\w+\-+)|(\w+\.))*\w{1,63}\.[a-zA-Z]{2,6})$/)
    
    if valid_email
      fromAddress = Entity.find_by_uuid("EMailManager").get_attr_val('fromAddress')
      @fromAddress = fromAddress.to_s.empty? ? 'reports@vmturbo.com' : fromAddress
      
      cmd = "mailx -a #{pdf_file_path} -r '#{@fromAddress}' -s 'VMTurbo: #{@user_report.title.to_s}' '#{params[:email_addresses].to_s.gsub(/[; ]/,',')}' < /dev/null"
      rc = system cmd
    end
    msg = valid_email ? (rc ? "Message sent " : ("Command execution returned false for: "+cmd)) : "Invalid email "
      
    render :inline => msg,
           :layout => false
  end

  # ---

  def view
    prepare_results()
    render :action => 'rows_as_html', :layout => 'basic'
  end


  def as_pdf
    prepare_results()

    pdf_file_path = make_pdf()

    send_file pdf_file_path, :type => "application/pdf"
  end
  
  def as_pdf_path
    prepare_results()

    pdf_file_path = make_pdf()
  
  render :text => pdf_file_path 
  end


  def as_xml
    prepare_results()

    xml = render_to_string :type => :builder,
           :inline => <<-STR
                        xml.instruct!
                        xml.result_set({ :title => @query.title}) {
                          for row in @rows do
                            xml.row {
                              @arr_fields.each{|f|
                                xml.method_missing(f,row.send(f))
                              }
                            }
                          end
                        }
                      STR

    FileUtils.mkdir_p "/tmp"
    outfile = "/tmp/#{@user_report.title.to_s.gsub(/\W/,'')}.xml"
    File.open(outfile,"w") {|f| f.write(xml)}

    send_file outfile, :type => "text/xml"
  end


  def as_csv
    prepare_results()

    csv_string = @arr_fields.collect{|f| f.titleize}.join(",") + "\n"
    if @rows.size > 0 then
      for record in @rows do
        csv_string << '"'+@arr_fields.collect{|s| record.send(s).to_s.gsub("\"","\"\"")}.join('","')+'"' + "\n"
      end
    end

    # send_data csv_string, :filename => 'user_report_data.csv', :type => 'text/csv'

    FileUtils.mkdir_p "/tmp"
    outfile = "/tmp/#{@user_report.title.to_s.gsub(/\W/,'')}.csv"
    File.open(outfile,"w") {|f| f.write(csv_string)}

    send_file outfile, :type => "text/csv"
  end


  ### -------------------------------------------


  def new
    @new_obj = @user_report = UserReport.new

    @user_report.title = "New Report"
    @user_report.category = "Capacity Management"
    @user_report.short_desc = @user_report.title
    @user_report.description = @user_report.title

    xml_str = ""
    xml = Builder::XmlMarkup.new(:indent => 2, :target => xml_str)
    xml.instruct!
    xml.user_report do
      xml.title @user_report.title
      xml.table "pm_stats_by_hour"
      xml.limit "500"
      xml.fields
      xml.filter_by
      xml.order_by
    end
    @user_report.xml_descriptor = xml_str

    @ft_subject = "User Report"
    @ft_activity = "New"

    if params && params[:redirect]
        session[:post_create_redirect] = params[:redirect]
    end

    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @user_report }
    end
  end

  # ---

  def create
    params[:user_report].delete_if{|k,v| ! ["title","category","short_desc","description"].include?(k)}
    params[:user_report][:xml_descriptor] = get_xml_descriptor()

    @new_obj = @user_report = UserReport.new(params[:user_report])

    if ! ok_to_add_or_update?
        flash[:notice] = 'Not added. ' + @reason.to_s
        @ft_subject = "User Report"
        @ft_activity = "New"
        render :action => 'new'
    else
      respond_to do |format|
        if @user_report.save
          format.xml  { render :xml => @user_report, :status => :created, :location => @user_report }
          format.html {
            flash[:notice] = "#{@user_report.iname} was successfully created."
            redirected_to_aggregator = update_aggregator(@user_report,params)
            if ( ! redirected_to_aggregator ) && session[:post_create_redirect]
                url = session[:post_create_redirect] + "?user_report_id=" + @user_report.id.to_s + "&user_report_ids[]=" + @user_report.id.to_s
                redirect_to url
                session[:post_create_redirect] = nil
            else
                redirect_to :id => @user_report.id, :action => (params[:commit].to_s.include?("Another") ? 'new' : 'edit') if ! redirected_to_aggregator
            end
          }
        else
          format.xml  { render :xml => @user_report.errors, :status => :unprocessable_entity }
          format.html {
            flash[:notice] = 'User Report could not be created. See errors.'
            render :action => 'new'
          }
        end
      end
    end
  end

  # ---

  def update
    @updated_obj = @user_report = UserReport.find(params[:id])

    params[:user_report].delete_if{|k,v| ! ["title","category","period","day_type","short_desc","description"].include?(k)}
    params[:user_report][:xml_descriptor] = get_xml_descriptor()

    respond_to do |format|
      if @user_report.update_attributes(params[:user_report])
        format.xml  { head :ok }
        format.html {
          flash[:notice] = 'User Report was successfully updated.'
          redirect_to :action => 'show', :id => @user_report
        }
      else
        format.xml  { render :xml => @user_report.errors, :status => :unprocessable_entity }
        format.html {
          redirect_to :action => 'edit', :id => @user_report
        }
      end
    end
  end


  ### -------------------------------------------

  def decode_xml()
    @query = STQuery.new_from_xml(@user_report.xml_descriptor.to_s)

    @query.title = @user_report.title
    @query.category = @user_report.category
    @query.short_desc = @user_report.short_desc
    @query.description = @user_report.description

    return @query
  end

  def as_sql()
    prepare_results()
  	@query = decode_xml()
    @sql = @query.to_sql( {} )
  	
  	puts @sql
  	
  	render :text => @sql
  end

  def fetch_results()
    @query = decode_xml()

    ###

    aliases = { }
    @sql = @query.to_sql(aliases)

    begin
      @rows = UserReport.find_by_sql( @sql )
    rescue
      @rows = UserReport.find(:all, :conditions => "title is null")
        # forces creation of an empty set that will convert to xml
    end
  end

  NumberPattern = /^[0-9]+(\.[0-9]+(E.*)?)?$/

  def prepare_results
    set_context()

    fetch_results()

    selected_fields = @query.fields.to_a.select{|f| f.is_selected}.sort{|a,b| a.col_order.to_i <=> b.col_order.to_i}
    @arr_fields = selected_fields.collect { |f| f.name }

    for r in @rows do
      for f in @arr_fields do
        v = r.send(f)
        # convert numbers to fixed point notation and remove unnecessary fractions
        if v =~ NumberPattern
          r.send("#{f}=",sprintf("%18.3f",v.to_f).strip.gsub(/^([1-9][0-9]+)\.000$/,'\1'))
        end
      end
    end
  end


  def get_resultset
    prepare_results()
    @show_links = true
    render :action => 'rows_as_html', :layout => false
  end


  ### -------------------------------------------

  def vis_edit
    set_context()
    @query = decode_xml()
    @selected_fields = @query.fields.to_a.select{|f| f.is_selected}.sort{|a,b| a.col_order.to_i <=> b.col_order.to_i}
  end


  def vis_edit_update
    set_context()
    @user_report.update_attributes(params[:user_report])

    @query = decode_xml()
    @query.limit = params[:limit].to_i

    @user_report.xml_descriptor = @query.to_xml()
    @user_report.save()

    redirect_to :controller => 'user_reports', :action => 'vis_edit', :id => @user_report
  end


  def move_field_up
    set_context()
    @query = decode_xml()

    field = @query.get_field(params[:field_name])
    field.col_order -= 1

    selected_fields = @query.fields.to_a.select{|f| f.is_selected}
    for other_field in (selected_fields - [field]) do
      other_field.col_order += 1 if other_field.col_order == field.col_order
    end

    @user_report.xml_descriptor = @query.to_xml()
    @user_report.save()

    redirect_to :action => 'vis_edit', :id => @user_report
  end


  def move_sort_up
    set_context()
    @query = decode_xml()

    field = @query.get_field(params[:field_name])
    field.sort_order -= 1

    selected_fields = @query.fields.to_a.select{|f| f.is_selected}
    for other_field in (selected_fields - [field]) do
      other_field.sort_order += 1 if other_field.sort_order == field.sort_order
    end

    @user_report.xml_descriptor = @query.to_xml()
    @user_report.save()

    redirect_to :action => 'vis_edit', :id => @user_report
  end


  def remove_field
    set_context()
    @query = decode_xml()

    field = @query.get_field(params[:field_name])
    if field
      field.is_selected = false
      field.sorting = nil

      @user_report.xml_descriptor = @query.to_xml()
      @user_report.save()
    end

    redirect_to :action => 'vis_edit', :id => @user_report
  end


  def add_field
    set_context()
    @query = decode_xml()

    field = @query.get_field(params[:field_name])
    field = @query.new_field(params[:field_name],true,99) if field.nil?
    field.is_selected = true
    field.col_order = 99

    @user_report.xml_descriptor = @query.to_xml()
    @user_report.save()

    redirect_to :action => 'vis_edit', :id => @user_report
  end


  def toggle_sort
    set_context()
    @query = decode_xml()

    field = @query.get_field(params[:field_name])
    if field
      field.sorting = {"" => 'asc', "asc" => 'desc'}[field.sorting.to_s.downcase]

      field.sort_order = 99 if field.sorting.to_s.downcase == "asc"

      @user_report.xml_descriptor = @query.to_xml()
      @user_report.save()
    end

    redirect_to :action => 'vis_edit', :id => @user_report
  end


  def remove_conditions
    set_context()
    @query = decode_xml()

    field = @query.get_field(params[:field_name])
    if field
      field.conditions = []

      @user_report.xml_descriptor = @query.to_xml()
      @user_report.save()
    end

    redirect_to :action => 'vis_edit', :id => @user_report
  end


  def add_condition
    set_context()
    @query = decode_xml()

    field = @query.get_field(params[:field_name])
    field = @query.new_field(params[:field_name],false,nil) if field.nil?

    if params[:operands].to_s[/[a-zA-Z_]+\(/]
      values = [ params[:operands].to_s ]
    else
      values = params[:operands].to_s.split(",")
    end

    values = values.collect{|v| v.gsub(/[;'"]/,'').strip}

    field.new_condition(params[:operation],values)

    @user_report.xml_descriptor = @query.to_xml()
    @user_report.save()

    redirect_to :action => 'vis_edit', :id => @user_report
  end


  def get_enum_vals
    set_context()
    @query = decode_xml()
    @values = ["no values found"]

    table = @query.table.to_s
    field = params[:field_name].to_s

    q_params = UserTablesController.params_for(table)

    if q_params then
      data_source = q_params[:enum_source]

      if field == 'instance_name'
        # users see "instance_name" but is an alias for "display_name"
        field = "display_name"
        entity_code = q_params[:ecode]
        if entity_code
          data_source = "#{entity_code}_instances"
        else
          data_source = "entities"
        end
      end

      begin
        sql = "select distinct #{field} as value from #{data_source} order by #{field}"
        @values = UserReport.find_by_sql(sql).collect{|rec| rec.value}
      rescue
        logger.error("*** Error while executing #{sql}")
      end
    else
      logger.error("*** Error - missing params for user table #{table}")
    end

    render :layout => false, :inline => <<-EOS
        <% for v in @values do %>
          <p style="margin:0 0 3px 0;"><%=v%></p>
        <% end %>
      EOS
  end

  ### -------------------------------------------

  def add_new_user_rept
    case
      when params[:table]
        create_from_table(params[:table])
        redirect_to :action => 'vis_edit', :id => @new_rept
        return
      when params[:rept_id]
        create_from_existing(params[:rept_id])
        redirect_to :action => 'vis_edit', :id => @new_rept
        return
    end

    @available_tables = UserTablesController::USER_TABLES.collect{|t| [t[:name],t[:purpose],t[:description]]}
    @available_repts = UserReport.find(:all, :order => "title").select{|r| ! r.is_obsolete?}
  end


  def create_from_table(table_name)
    @new_rept = UserReport.new()
    @new_rept.title = "New for "+table_name
    @new_rept.category = "Capacity Management"
    @new_rept.short_desc = @new_rept.title
    @new_rept.description = @new_rept.title
    xml_str = ""
    xml = Builder::XmlMarkup.new(:indent => 2, :target => xml_str)
    xml.instruct!
    xml.user_report do
      xml.title @new_rept.title
      xml.table table_name
      xml.limit "500"
      xml.fields
      xml.filter_by
      xml.order_by
    end
    @new_rept.xml_descriptor = xml_str
    @new_rept.save()
  end


  def create_from_existing(rept_id)
    @rept = UserReport.find(rept_id)
    @new_rept = @rept.clone()
    @new_rept.title = ("copy of "+@rept.title.to_s)[0,UserReport.attr_type_info('title')[:length]]
    @new_rept.save()
  end


  def destroy_rept
    set_context()
    @user_report.destroy

    redirect_to :action => 'list'
  end

#END-UID.usermethods

end