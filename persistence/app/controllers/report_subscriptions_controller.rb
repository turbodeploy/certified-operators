class ReportSubscriptionsController < ApplicationController

  before_filter :login_required

  def index
    prepare_list()
    respond_to do |format|
      format.html {
        render :action => @ft_custom_action and return if @ft_custom_action
        render :action => 'list'
      }
      format.xml  { render :xml => @report_subscriptions }
    end
  end

  def apply_to_checked_items
    if params[:action_to_perform] == 'Delete'    
        ReportSubscription.destroy(params[:delchk].keys) if params[:delchk] && params[:delchk].keys.size > 0
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
    ReportSubscription.update_all("#{params[:field]} = '#{params[:new_value].strip}'", where_clause)
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
    ReportSubscription.delete_all(where_clause)
    redirect_to :back
  end

  def quick_add_items
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      for str_name in names do
        str_name.strip!
        ReportSubscription.find_or_create_by_email(str_name)
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
      format.xml  { render :xml => @report_subscriptions }
    end
  end

  def block_list
    prepare_list()
    render :action => @ft_custom_action if @ft_custom_action
  end

  def new
    @new_obj = @report_subscription = ReportSubscription.new

    @ft_subject = "Report Subscription"
    @ft_activity = "New"

    if params && params[:redirect] 
        session[:post_create_redirect] = params[:redirect]
    end

    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @report_subscription }
    end
  end

  def set_context(load_related=true)
    @report_subscription = ReportSubscription.find(params[:id])

    @ft_subject = "Report Subscription"
    @ft_activity = @report_subscription.iname
    
    load_related = false if (params[:action] == "show_properties")    # don't load related items if only showing properties
    return unless load_related

    @standard_report = @report_subscription.standard_report

    @custom_report = @report_subscription.custom_report

    @on_demand_report = @report_subscription.on_demand_report

  end

  def show
    set_context()
    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @report_subscription }
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
    if params[:relname].to_s.underscore == 'standard_report' then
      render :xml => @report_subscription.standard_report || { }
      return
    end
    if params[:relname].to_s.underscore == 'custom_report' then
      render :xml => @report_subscription.custom_report || { }
      return
    end
    if params[:relname].to_s.underscore == 'on_demand_report' then
      render :xml => @report_subscription.on_demand_report || { }
      return
    end
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
    sanitize_params(:report_subscription)
    @new_obj = @report_subscription = ReportSubscription.new(params[:report_subscription])

    if ! ok_to_add_or_update?
        flash[:notice] = 'Not added. ' + @reason.to_s
        @ft_subject = "Report Subscription"
        @ft_activity = "New"
        render :action => 'new'
    else    
      respond_to do |format|
        if @report_subscription.save
          format.xml  { render :xml => @report_subscription, :status => :created, :location => @report_subscription }
          format.html {
            flash[:notice] = "#{@report_subscription.iname} was successfully created."
            redirected_to_aggregator = update_aggregator(@report_subscription,params)
            if ( ! redirected_to_aggregator ) && session[:post_create_redirect]
                redirect_to :controller=>'standard_reports', :action=>'list', :report_subscription_id=>@report_subscription.id.to_s, :'report_subscription_ids[]'=>@report_subscription.id.to_s
                session[:post_create_redirect] = nil
            else 
                redirect_to :id => @report_subscription.id, :action => (params[:commit].to_s.include?("Another") ? 'new' : 'edit') if ! redirected_to_aggregator 
            end
          }
        else
          format.xml  { render :xml => @report_subscription.errors, :status => :unprocessable_entity }
          format.html {
            flash[:notice] = 'Report Subscription could not be created. See errors.'
            render :action => 'new'
          }
        end
      end
    end
  end

  def update
    @updated_obj = @report_subscription = ReportSubscription.find(params[:id])
    sanitize_params(:report_subscription)

    if ! ok_to_add_or_update?
      flash[:notice] = 'Not updated. ' + @reason.to_s
      @ft_subject = "Report Subscription"
      @ft_activity = "Edit"
      redirect_to :action => 'edit', :id => @report_subscription and return
    end 

      respond_to do |format|
      if @report_subscription.update_attributes(params[:report_subscription])

        if session[:post_update_redirect] then
          redirect_to session[:post_update_redirect]
          session[:post_update_redirect] = nil
          return
        end

        format.xml  { head :ok }
        format.html {
          flash[:notice] = 'Report Subscription was successfully updated.'
          redirect_to :action => 'show', :id => @report_subscription
        }
      else
        format.xml  { render :xml => @report_subscription.errors, :status => :unprocessable_entity }
        format.html { 
          render :inline => "Invalid email address", :layout => 'empty'
        }
      end
    end
  end

  def ok_to_add_or_update?
    if params[:report_subscription][:email].to_s.empty?
      @reason = 'The email field can not be blank.'
      return false
    end
    return true
  end

  def destroy
    obj = ReportSubscription.find(params[:id])
    obj.destroy
    respond_to do |format|
      format.html {
        redirect_to :back
      }
      format.xml  { head :ok }
    end
  end


  # -------------------------------------------------

  def select_standard_report

    @ft_subject = "Report Subscription"
    @ft_activity = "Select Standard Report"

    @report_subscription = ReportSubscription.find(params[:id])
    @standard_reports = StandardReport.find(:all, :order => 'filename')
    @standard_reports = @standard_reports - [ @report_subscription ] 
    @my_standard_report = @report_subscription.standard_report
    @standard_reports = @standard_reports - [ @my_standard_report ]
  end
  
  def update_standard_report
    @report_subscription = ReportSubscription.find(params[:id])
    @standard_report = StandardReport.find(params[:standard_report_id]) if params[:standard_report_id] && params[:standard_report_id] != ""
    @report_subscription.standard_report = @standard_report ? @standard_report : nil
    @report_subscription.save
    redirect_to :controller => @report_subscription.ctrlr_name, :action => 'edit', :id => @report_subscription, :anchor => 'standard_report'
  end

  def remove_standard_report
    @report_subscription = ReportSubscription.find(params[:id])
    @report_subscription.standard_report = nil
    @report_subscription.save
    redirect_to :controller => @report_subscription.ctrlr_name, :action => 'edit', :id => @report_subscription, :anchor => 'standard_report'
  end

  # -------------------------------------------------

  def select_custom_report

    @ft_subject = "Report Subscription"
    @ft_activity = "Select User Report"

    @report_subscription = ReportSubscription.find(params[:id])
    @user_reports = UserReport.find(:all, :order => 'title')
    @user_reports = @user_reports - [ @report_subscription ] 
    @my_custom_report = @report_subscription.custom_report
    @user_reports = @user_reports - [ @my_custom_report ]
  end
  
  def update_custom_report
    @report_subscription = ReportSubscription.find(params[:id])
    @custom_report = UserReport.find(params[:user_report_id]) if params[:user_report_id] && params[:user_report_id] != ""
    @report_subscription.custom_report = @custom_report ? @custom_report : nil
    @report_subscription.save
    redirect_to :controller => @report_subscription.ctrlr_name, :action => 'edit', :id => @report_subscription, :anchor => 'custom_report'
  end

  def remove_custom_report
    @report_subscription = ReportSubscription.find(params[:id])
    @report_subscription.custom_report = nil
    @report_subscription.save
    redirect_to :controller => @report_subscription.ctrlr_name, :action => 'edit', :id => @report_subscription, :anchor => 'custom_report'
  end

  # -------------------------------------------------

  def select_on_demand_report

    @ft_subject = "Report Subscription"
    @ft_activity = "Select On Demand Report"

    @report_subscription = ReportSubscription.find(params[:id])
    @on_demand_reports = OnDemandReport.find(:all, :order => 'filename')
    @on_demand_reports = @on_demand_reports - [ @report_subscription ] 
    @my_on_demand_report = @report_subscription.on_demand_report
    @on_demand_reports = @on_demand_reports - [ @my_on_demand_report ]
  end
  
  def update_on_demand_report
    @report_subscription = ReportSubscription.find(params[:id])
    @on_demand_report = OnDemandReport.find(params[:on_demand_report_id]) if params[:on_demand_report_id] && params[:on_demand_report_id] != ""
    @report_subscription.on_demand_report = @on_demand_report ? @on_demand_report : nil
    @report_subscription.save
    redirect_to :controller => @report_subscription.ctrlr_name, :action => 'edit', :id => @report_subscription, :anchor => 'on_demand_report'
  end

  def remove_on_demand_report
    @report_subscription = ReportSubscription.find(params[:id])
    @report_subscription.on_demand_report = nil
    @report_subscription.save
    redirect_to :controller => @report_subscription.ctrlr_name, :action => 'edit', :id => @report_subscription, :anchor => 'on_demand_report'
  end

  def csv_upload
    # no need to prepare any data - just show the view to post against 'upload_csv'
  end

  def upload_csv

    # commented out - instead try to load the csv without an intermediate file
    #fn = "#{RAILS_ROOT}/uploads/report_subscription_data.csv"
    #File.open(fn, "wb") { |f| f.write(params[:csv_file].read) }
    #ReportSubscription.load_csv_file(fn) if ReportSubscription.respond_to? :load_csv_file

    ReportSubscription.load_csv_str(params[:csv_file].read) if ReportSubscription.respond_to? :load_csv_str
    
    redirect_to :action => 'list' 
  end

  def download_csv
    attr_names = ReportSubscription.value_attrs

    adjacent_objs = [:standard_report,:custom_report,:on_demand_report]
    attr_names << adjacent_objs.collect{|c| c.to_s}
    attr_names = attr_names.flatten

    csv_string = String.new
    csv_string = attr_names.join(",") + "\n"

    # override visual pagination establishing the limit to 100,000 rows
    params[:page] = '1'
    params[:page_length] = '100000'

    prepare_list()

    all_objs = @report_subscriptions      
    
    if all_objs.size > 0 then
      all_objs.each do |record| 
        csv_string << '"'+attr_names.collect{|s| record.get_attr_val(s,'csv').to_s.gsub("\"","\"\"")}.join('","')+'"' + "\n"
      end
    end
    send_data csv_string, :filename => 'report_subscription_data.csv', :type => 'text/csv'
  end 

  # --- 

  def show_queries
    @ft_subject = "ReportSubscription"
    @ft_activity  = "Reports"

    @queries = ReportSubscription.query_specs

    render :partial => 'fast_ops/queries', :layout => 'application'
  end

  def run_query
    @def_page_size ||= 20
    @page_size = params[:page_length].to_i > 0 ? params[:page_length].to_i : @def_page_size

    @page_no = [ params[:page].to_i, 1 ].max
    @page_clause = nil # "LIMIT #{@page_size.to_s} OFFSET #{((@page_no-1)*@page_size).to_s}"

    @query = ReportSubscription.query_specs[params[:query]]
    @sort_fields = params[:order_by] || (! @query[:order_by].to_s.empty? && @query[:order_by].join(",")) || @query[:cols][0]
    @where_clause = params[:where] || (@query[:where].to_s.length > 0 ? @query[:where] : nil)

    @base_sql = @query[:sql] + (@where_clause ? " WHERE #{@where_clause}" : "") 
    @count = ReportSubscription.count_by_sql("SELECT COUNT(id) FROM (#{@base_sql}) AS row_data")
    
    @sql = @base_sql + (@sort_fields ? " ORDER BY #{@sort_fields}" : "") + (@page_clause ? " #{@page_clause}" : "") 
    @rows = ReportSubscription.find_by_sql(apply_limit(@sql))

    @cols = @query[:cols] || (@rows.size > 0 && @rows[0].attributes.keys)

    @link_params = { :controller => 'report_subscriptions', :action => 'show' }

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

    @ft_subject  = ( @ft_subject || "Report Subscription".pluralize )
    @ft_activity = ( @ft_activity || "All" )

    @order_field = ( params[:order_by] || "email" )
    @direction = ( params[:direction] || "ASC" )

    @query_param = ( params[:query] || "list_all" )
    params[:query] = @query_param
    
    params[:order_by] = quoted_name(@order_field) + " " + @direction

    @where_param = params[:where]  # save the original where param

    adjust_sorting_and_filtering_params(ReportSubscription,params)
    @order_field = params[:sort_field].to_s unless params[:sort_field].to_s.empty?

    @report_subscriptions = run_query()

    # restore original params passed to the server
    params.clear
    params.merge!(orig_params)
  end

  # --- 

#BEGIN-UID.usermethods

  before_filter :redirect_to_list, :only => [ :list ]
  def redirect_to_list
    session[:post_create_redirect] = url_for(:controller => 'report_subscriptions', :action => 'list')
  end

  before_filter :edit_settings, :only => [ :edit ]
  def edit_settings
    @embedded = (params[:usermode].to_s == 'true')
  end

  before_filter :adjust_params, :only => [ :create, :update ]
  def adjust_params
    if params[:report_subscription][:period].to_s == "Weekly" &&
       params[:report_subscription][:day_type].to_s.include?("--") then
      params[:report_subscription][:day_type] = "Sat"
    end

    if params[:report_subscription][:period].to_s != "Weekly"
      params[:report_subscription][:day_type] = ''
    end

    if params[:report_subscription][:email].to_s.empty?
      params[:report_subscription][:email] = "EMPTY"
    end
  end

  after_filter :kill_empties, :only => [ :create, :update ]
  def kill_empties
    if @report_subscription.email == "EMPTY"
      @report_subscription.destroy
    end
  end

  # # #

  def before_prepare_list
    @def_page_size = 200
    params[:page_length] = 200

    do_host_reports    = StandardReport.host_feature_enabled?
    do_storage_reports = StandardReport.storage_feature_enabled?
    do_planner_reports = StandardReport.planner_feature_enabled?
    do_custom_reports  = StandardReport.custom_reports_feature_enabled?

    @reports = StandardReport.find(:all)

    @reports = @reports.select{|rept| rept.user_selected?}

    @reports = @reports.select{|rept|
      (rept.is_a_host_report? && do_host_reports) ||
      (rept.is_a_storage_report? && do_storage_reports) ||
      (rept.is_a_planner_report? && do_planner_reports)
    }

    if do_custom_reports
      @reports += UserReport.find(:all).reject { |ur| ur.is_obsolete? }
    end

    @reports.sort!{|a,b| a.title <=> b.title}
  end

  # # #

  def edit_subscriptions_form
    @report_subscription = ReportSubscription.new()
    if params[:class_name] == "StandardReport" then
      @report_subscription.standard_report = StandardReport.find(params[:id])
    else
      @report_subscription.custom_report = UserReport.find(params[:id])
    end

    render :inline => <<-EOS
      <div style="margin-top:6px; border:outset; padding:10px; background:#BBDDEE;">
      <%= form_tag :controller => "report_subscriptions", :action => "create" %>
      <% @edit_ctrl_cols = 30 %>
      <%= render :partial => "report_subscriptions/form_small" %>
      <p>&nbsp;</p>
      <%= submit_tag "Save"%> &nbsp; &nbsp; <%= js_button_to "Cancel", params[:return_to]%>
      </form>
      </div>
      EOS

  end


  # # #

  def edit_on_demand

    session[:post_update_redirect] = url_for(params)

    @on_demand_report = OnDemandReport.find_by_filename(params[:filename].to_s)
	filter = params[:filter]
    if @on_demand_report then
      @report_subscriptions = @on_demand_report.report_subscriptions.find(:all)

      if @on_demand_report.scope_type == 'Instance'
        @entities = Entity.find(:all, :conditions => "creation_class = '#{@on_demand_report.obj_type.to_s}'", :order => "display_name")
      else
      	filterCond = (!filter.nil? && !filter.empty?) ? "and internal_name like '#{filter}%'" : ""
      	baseQuery = "select entity_id as id, group_name as display_name, group_uuid as uuid from entity_groups where group_type = '#{@on_demand_report.obj_type.to_s}' #{filterCond} order by display_name"
      	puts baseQuery
        @entities = Entity.find_by_sql(baseQuery)
      end

      @entity_from_uuid = @report_subscriptions.inject({}){|map,rs| map[rs.obj_uuid] = @entities.find{|e| e.uuid == rs.obj_uuid}; map}

      render :layout => 'empty'
    else
      render :inline => "Filename does not match an on-demand report: <%= params[:filename].to_s %>", :layout => 'empty'
    end
  end


  def create_on_demand

    begin
      unless params[:entity_uuids].nil? || params[:report_subscription][:email].to_s.empty?
        @on_demand_report = OnDemandReport.find(params[:on_demand_report_id])

        if params[:report_subscription][:period].to_s == 'Weekly'
          params[:report_subscription][:day_type] = 'Sat' if params[:report_subscription][:day_type].to_s.empty?
        else
          params[:report_subscription][:day_type] = ''
        end

        for uuid in params[:entity_uuids].keys do
          subscr = ReportSubscription.new(params[:report_subscription].merge({:obj_uuid => uuid}))
          subscr.on_demand_report = @on_demand_report
          subscr.save()
        end
      end
    rescue => e
      logger.error("*** Error: Exception while creation on-demand report subscriptions")
      logger.error("*** Error details: #{e.message}")
    end

    redirect_to :back

    # render :inline => "<%= params.inspect%>"
  end

#END-UID.usermethods

end

