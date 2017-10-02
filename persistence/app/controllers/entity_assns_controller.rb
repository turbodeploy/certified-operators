class EntityAssnsController < ApplicationController
  before_filter :login_required

  def index
    prepare_list()
    respond_to do |format|
      format.html {
        render :action => @ft_custom_action and return if @ft_custom_action
        render :action => 'list'
      }
      format.xml  { render :xml => @entity_assns }
    end
  end

  def apply_to_checked_items
    if params[:action_to_perform] == 'Delete'    
        EntityAssn.destroy(params[:delchk].keys) if params[:delchk] && params[:delchk].keys.size > 0
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
    EntityAssn.update_all("#{params[:field]} = '#{params[:new_value].strip}'", where_clause)
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
    EntityAssn.delete_all(where_clause)
    redirect_to :back
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
      format.xml  { render :xml => @entity_assns }
    end
  end

  def block_list
    prepare_list()
    render :action => @ft_custom_action if @ft_custom_action
  end

  def new
    @new_obj = @entity_assn = EntityAssn.new

    @ft_subject = "Entity Assn"
    @ft_activity = "New"

    if params && params[:redirect] 
        session[:post_create_redirect] = params[:redirect]
    end

    @entity = Entity.find(params[:entity_id]) if params[:entity_id]
    @new_obj.entity = @entity
    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @entity_assn }
    end
  end

  def set_context(load_related=true)
    @entity_assn = EntityAssn.find(params[:id])

    @ft_subject = "Entity Assn"
    @ft_activity = @entity_assn.iname
    
    load_related = false if (params[:action] == "show_properties")    # don't load related items if only showing properties
    return unless load_related

    @order_field = get_order_field(params,'entity_assn','members','name')
    @id_set = @entity_assn.send("#{'members'.singularize}_ids").concat(["0"]).join(",")
    @members = Entity.find_by_sql(%Q~select * from #{Entity.default_query("entity.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

    @entity = @entity_assn.entity

  end

  def show
    set_context()
    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @entity_assn }
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
    if params[:relname].to_s.underscore == 'members' then
      render :xml => @entity_assn.members.find(:all)
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
    @entity = Entity.find(params[:entity_id]) if params[:entity_id] && params[:entity_id].length > 0
    if @entity 
      @entity.associations << new_obj
      unless session[:post_create_redirect]
        if ! params[:commit].to_s.include?("Another")
          redirect_to :controller => @entity.ctrlr_name, :action => 'edit', :id => @entity.id, :anchor => "associations"
        else
          redirect_to :action => 'new', :params => { :entity_id => @entity.id }
        end
        return true
      end
    end
    return false
  end

  def sanitize_params(hk)
    params[hk].each{|k,v| params[hk][k] = v.gsub("-- select --","")}  if params[hk]
    params[hk].each{|k,v| params[hk][k] = v.gsub(/HH:MM.*/,"")}       if params[hk]
  end

  def create
    sanitize_params(:entity_assn)
    @new_obj = @entity_assn = EntityAssn.new(params[:entity_assn])

    if ! ok_to_add_or_update?
        flash[:notice] = 'Not added. ' + @reason.to_s
        @ft_subject = "Entity Assn"
        @ft_activity = "New"
        @entity = Entity.find(params[:entity_id]) if params[:entity_id] && Entity.exists?(params[:entity_id])
        render :action => 'new'
    else    
      respond_to do |format|
        if @entity_assn.save
          format.xml  { render :xml => @entity_assn, :status => :created, :location => @entity_assn }
          format.html {
            flash[:notice] = "#{@entity_assn.iname} was successfully created."
            redirected_to_aggregator = update_aggregator(@entity_assn,params)
            if ( ! redirected_to_aggregator ) && session[:post_create_redirect]
                url = session[:post_create_redirect] + "?entity_assn_id=" + @entity_assn.id.to_s + "&entity_assn_ids[]=" + @entity_assn.id.to_s 
                redirect_to url
                session[:post_create_redirect] = nil
            else 
                redirect_to :id => @entity_assn.id, :action => (params[:commit].to_s.include?("Another") ? 'new' : 'edit') if ! redirected_to_aggregator 
            end
          }
        else
          format.xml  { render :xml => @entity_assn.errors, :status => :unprocessable_entity }
          format.html {
            flash[:notice] = 'Entity Assn could not be created. See errors.'
            @entity = Entity.find(params[:entity_id]) if params[:entity_id] && Entity.exists?(params[:entity_id])
            render :action => 'new'
          }
        end
      end
    end
  end

  def update
    @updated_obj = @entity_assn = EntityAssn.find(params[:id])
    sanitize_params(:entity_assn)

    if ! ok_to_add_or_update?
      flash[:notice] = 'Not updated. ' + @reason.to_s
      @ft_subject = "Entity Assn"
      @ft_activity = "Edit"
      redirect_to :action => 'edit', :id => @entity_assn and return
    end 

      respond_to do |format|
      if @entity_assn.update_attributes(params[:entity_assn])

        if session[:post_update_redirect] then
          redirect_to session[:post_update_redirect]
          session[:post_update_redirect] = nil
          return
        end

        format.xml  { head :ok }
        format.html {
          flash[:notice] = 'Entity Assn was successfully updated.'
          redirect_to :action => 'show', :id => @entity_assn
        }
      else
        format.xml  { render :xml => @entity_assn.errors, :status => :unprocessable_entity }
        format.html { 
          redirect_to :action => 'edit', :id => @entity_assn
        }
      end
    end
  end

  def ok_to_add_or_update?
    if params[:entity_assn][:name].to_s.empty?
      @reason = 'The name field can not be blank.'
      return false
    end
    return true
  end

  def destroy
    obj = EntityAssn.find(params[:id])
    obj.destroy
    respond_to do |format|
      format.html {
        redirect_to :back
      }
      format.xml  { head :ok }
    end
  end


  # -------------------------------------------------

  def add_to_members
    @ft_subject = "Entity Assn"
    @ft_activity = "Select Entity".pluralize

    @entity_assn = EntityAssn.find(params[:id])
    @entities = Entity.choices_for(@entity_assn,'members')
    @entities = @entities - [ @entity_assn ] 
    @my_members = @entity_assn.members
    @entities.delete_if { |a| @my_members.include?(a) }
  end
  
  def update_members
    @entity_assn = EntityAssn.find(params[:id])
    @members = [ Entity.find(params[:entity_ids]) ] if params[:entity_ids]
    @entity_assn.members << @members if @members
    redirect_to :controller => @entity_assn.ctrlr_name, :action => 'edit', :id => @entity_assn, :anchor => 'members'
  end

  def remove_from_members
    @entity_assn = EntityAssn.find(params[:id])
    @members = Entity.find(params[:entity_id])
    @entity_assn.members.delete @members
    redirect_to :controller => @entity_assn.ctrlr_name, :action => 'edit', :id => @entity_assn, :anchor => 'members'
  end

  def quick_add_members
    @entity_assn = EntityAssn.find(params[:id])
    
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      existing_entities = @entity_assn.members.find(:all)
      for str_name in names do
        str_name.strip!
        new_obj = Entity.find_or_create_by_name(str_name)
        @entity_assn.members << new_obj unless existing_entities.include?(new_obj)
      end
    end
    
    redirect_to :action => 'edit', :id => @entity_assn.id
  end


  def csv_upload
    # no need to prepare any data - just show the view to post against 'upload_csv'
  end

  def upload_csv

    # commented out - instead try to load the csv without an intermediate file
    #fn = "#{RAILS_ROOT}/uploads/entity_assn_data.csv"
    #File.open(fn, "wb") { |f| f.write(params[:csv_file].read) }
    #EntityAssn.load_csv_file(fn) if EntityAssn.respond_to? :load_csv_file

    EntityAssn.load_csv_str(params[:csv_file].read) if EntityAssn.respond_to? :load_csv_str
    
    redirect_to :action => 'list' 
  end

  def download_csv
    attr_names = EntityAssn.value_attrs

    adjacent_objs = [:entity]
    attr_names << adjacent_objs.collect{|c| c.to_s}
    attr_names = attr_names.flatten

    csv_string = String.new
    csv_string = attr_names.join(",") + "\n"

    # override visual pagination establishing the limit to 100,000 rows
    params[:page] = '1'
    params[:page_length] = '100000'

    prepare_list()

    all_objs = @entity_assns      
    
    if all_objs.size > 0 then
      all_objs.each do |record| 
        csv_string << '"'+attr_names.collect{|s| record.get_attr_val(s,'csv').to_s.gsub("\"","\"\"")}.join('","')+'"' + "\n"
      end
    end
    send_data csv_string, :filename => 'entity_assn_data.csv', :type => 'text/csv'
  end 

  # --- 

  def show_queries
    @ft_subject = "EntityAssn"
    @ft_activity  = "Reports"

    @queries = EntityAssn.query_specs

    render :partial => 'fast_ops/queries', :layout => 'application'
  end

  def run_query
    @def_page_size ||= 20
    @page_size = params[:page_length].to_i > 0 ? params[:page_length].to_i : @def_page_size

    @page_no = [ params[:page].to_i, 1 ].max
    @page_clause = nil # "LIMIT #{@page_size.to_s} OFFSET #{((@page_no-1)*@page_size).to_s}"

    @query = EntityAssn.query_specs[params[:query]]
    @sort_fields = params[:order_by] || (! @query[:order_by].to_s.empty? && @query[:order_by].join(",")) || @query[:cols][0]
    @where_clause = params[:where] || (@query[:where].to_s.length > 0 ? @query[:where] : nil)

    @base_sql = @query[:sql] + (@where_clause ? " WHERE #{@where_clause}" : "") 
    @count = EntityAssn.count_by_sql("SELECT COUNT(id) FROM (#{@base_sql}) AS row_data")
    
    @sql = @base_sql + (@sort_fields ? " ORDER BY #{@sort_fields}" : "") + (@page_clause ? " #{@page_clause}" : "") 
    @rows = EntityAssn.find_by_sql(apply_limit(@sql))

    @cols = @query[:cols] || (@rows.size > 0 && @rows[0].attributes.keys)

    @link_params = { :controller => 'entity_assns', :action => 'show' }

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

    @ft_subject  = ( @ft_subject || "Entity Assn".pluralize )
    @ft_activity = ( @ft_activity || "All" )

    @order_field = ( params[:order_by] || "name" )
    @direction = ( params[:direction] || "ASC" )

    @query_param = ( params[:query] || "list_all" )
    params[:query] = @query_param
    
    params[:order_by] = quoted_name(@order_field) + " " + @direction

    @where_param = params[:where]  # save the original where param

    adjust_sorting_and_filtering_params(EntityAssn,params)
    @order_field = params[:sort_field].to_s unless params[:sort_field].to_s.empty?

    @entity_assns = run_query()

    # restore original params passed to the server
    params.clear
    params.merge!(orig_params)
  end

  # --- 

#BEGIN-UID.usermethods

#END-UID.usermethods

end

