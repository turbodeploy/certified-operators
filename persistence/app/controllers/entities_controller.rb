class EntitiesController < ApplicationController
  before_filter :login_required

  def index
    prepare_list()
    respond_to do |format|
      format.html {
        render :action => @ft_custom_action and return if @ft_custom_action
        render :action => 'list'
      }
      format.xml  { render :xml => @entities }
    end
  end

  def apply_to_checked_items
    if params[:action_to_perform] == 'Delete'    
        Entity.destroy(params[:delchk].keys) if params[:delchk] && params[:delchk].keys.size > 0
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
    Entity.update_all("#{params[:field]} = '#{params[:new_value].strip}'", where_clause)
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
    Entity.delete_all(where_clause)
    redirect_to :back
  end

  def quick_add_items
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      for str_name in names do
        str_name.strip!
        Entity.find_or_create_by_name(str_name)
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
      format.xml  { render :xml => @entities }
    end
  end

  def block_list
    prepare_list()
    render :action => @ft_custom_action if @ft_custom_action
  end

  def new
    @new_obj = @entity = Entity.new

    @ft_subject = "Entity"
    @ft_activity = "New"

    if params && params[:redirect] 
        session[:post_create_redirect] = params[:redirect]
    end

    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @entity }
    end
  end

  def set_context(load_related=true)
    @entity = Entity.find(params[:id])

    @ft_subject = "Entity"
    @ft_activity = @entity.iname
    
    load_related = false if (params[:action] == "show_properties")    # don't load related items if only showing properties
    return unless load_related

    @order_field = get_order_field(params,'entity','associations','name')
    @id_set = @entity.send("#{'associations'.singularize}_ids").concat(["0"]).join(",")
    @associations = EntityAssn.find_by_sql(%Q~select * from #{EntityAssn.default_query("entity_assn.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

    @order_field = get_order_field(params,'entity','attrs','name')
    @id_set = @entity.send("#{'attrs'.singularize}_ids").concat(["0"]).join(",")
    @attrs = EntityAttr.find_by_sql(%Q~select * from #{EntityAttr.default_query("entity_attr.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

    @order_field = get_order_field(params,'entity','member_associations','name')
    @id_set = @entity.send("#{'member_associations'.singularize}_ids").concat(["0"]).join(",")
    @member_associations = EntityAssn.find_by_sql(%Q~select * from #{EntityAssn.default_query("entity_assn.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

    @order_field = get_order_field(params,'entity','classifications','name')
    @id_set = @entity.send("#{'classifications'.singularize}_ids").concat(["0"]).join(",")
    @classifications = Classification.find_by_sql(%Q~select * from #{Classification.default_query("classification.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

  end

  def show
    set_context()
    respond_to do |format|
      format.html { render :action => @ft_custom_action if @ft_custom_action }
      format.xml  { render :xml => @entity }
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
    if params[:relname].to_s.underscore == 'associations' then
      render :xml => @entity.associations.find(:all)
      return
    end
    if params[:relname].to_s.underscore == 'attrs' then
      render :xml => @entity.attrs.find(:all)
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
    sanitize_params(:entity)
    @new_obj = @entity = Entity.new(params[:entity])

    if ! ok_to_add_or_update?
        flash[:notice] = 'Not added. ' + @reason.to_s
        @ft_subject = "Entity"
        @ft_activity = "New"
        render :action => 'new'
    else    
      respond_to do |format|
        if @entity.save
          format.xml  { render :xml => @entity, :status => :created, :location => @entity }
          format.html {
            flash[:notice] = "#{@entity.iname} was successfully created."
            redirected_to_aggregator = update_aggregator(@entity,params)
            if ( ! redirected_to_aggregator ) && session[:post_create_redirect]
                url = session[:post_create_redirect] + "?entity_id=" + @entity.id.to_s + "&entity_ids[]=" + @entity.id.to_s 
                redirect_to url
                session[:post_create_redirect] = nil
            else 
                redirect_to :id => @entity.id, :action => (params[:commit].to_s.include?("Another") ? 'new' : 'edit') if ! redirected_to_aggregator 
            end
          }
        else
          format.xml  { render :xml => @entity.errors, :status => :unprocessable_entity }
          format.html {
            flash[:notice] = 'Entity could not be created. See errors.'
            render :action => 'new'
          }
        end
      end
    end
  end

  def update
    @updated_obj = @entity = Entity.find(params[:id])
    sanitize_params(:entity)

    if ! ok_to_add_or_update?
      flash[:notice] = 'Not updated. ' + @reason.to_s
      @ft_subject = "Entity"
      @ft_activity = "Edit"
      redirect_to :action => 'edit', :id => @entity and return
    end 

      respond_to do |format|
      if @entity.update_attributes(params[:entity])

        if session[:post_update_redirect] then
          redirect_to session[:post_update_redirect]
          session[:post_update_redirect] = nil
          return
        end

        format.xml  { head :ok }
        format.html {
          flash[:notice] = 'Entity was successfully updated.'
          redirect_to :action => 'show', :id => @entity
        }
      else
        format.xml  { render :xml => @entity.errors, :status => :unprocessable_entity }
        format.html { 
          redirect_to :action => 'edit', :id => @entity
        }
      end
    end
  end

  def ok_to_add_or_update?
    if params[:entity][:name].to_s.empty?
      @reason = 'The name field can not be blank.'
      return false
    end
    return true
  end

  def destroy
    obj = Entity.find(params[:id])
    obj.destroy
    respond_to do |format|
      format.html {
        redirect_to :back
      }
      format.xml  { head :ok }
    end
  end


  # -------------------------------------------------

  def quick_add_associations
    @entity = Entity.find(params[:id])
    
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      existing_entity_assns = @entity.associations.find(:all)
      for str_name in names do
        str_name.strip!
        new_obj = EntityAssn.find_or_create_by_name(str_name)
        @entity.associations << new_obj unless existing_entity_assns.include?(new_obj)
      end
    end
    
    redirect_to :action => 'edit', :id => @entity.id
  end


  # -------------------------------------------------

  def quick_add_attrs
    @entity = Entity.find(params[:id])
    
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      existing_entity_attrs = @entity.attrs.find(:all)
      for str_name in names do
        str_name.strip!
        new_obj = EntityAttr.find_or_create_by_name(str_name)
        @entity.attrs << new_obj unless existing_entity_attrs.include?(new_obj)
      end
    end
    
    redirect_to :action => 'edit', :id => @entity.id
  end


  # -------------------------------------------------

  def add_to_member_associations
    @ft_subject = "Entity"
    @ft_activity = "Select Entity Assn".pluralize

    @entity = Entity.find(params[:id])
    @entity_assns = EntityAssn.choices_for(@entity,'member_associations')
    @entity_assns = @entity_assns - [ @entity ]
    @my_member_associations = @entity.member_associations
    @entity_assns.delete_if{|a| @my_member_associations.include?(a)}
  end
  
  def update_member_associations
    @entity = Entity.find(params[:id])
    @member_associations = [ EntityAssn.find(params[:entity_assn_ids]) ] if params[:entity_assn_ids]
    @entity.member_associations << @member_associations if @member_associations
    redirect_to :controller => @entity.ctrlr_name, :action => 'edit', :id => @entity, :anchor => 'member_associations'
  end

  def remove_from_member_associations
    @entity = Entity.find(params[:id])
    @member_associations = EntityAssn.find(params[:entity_assn_id])
    @entity.member_associations.delete @member_associations
    redirect_to :controller => @entity.ctrlr_name, :action => 'edit', :id => @entity, :anchor => 'member_associations'
  end

  def quick_add_member_associations
    @entity = Entity.find(params[:id])
    
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      existing_entity_assns = @entity.member_associations.find(:all)
      for str_name in names do
        str_name.strip!
        new_obj = EntityAssn.find_or_create_by_name(str_name)
        @entity.member_associations << new_obj unless existing_entity_assns.include?(new_obj)
      end
    end
    
    redirect_to :action => 'edit', :id => @entity.id
  end


  # -------------------------------------------------

  def add_to_classifications
    @ft_subject = "Entity"
    @ft_activity = "Select Classification".pluralize

    @entity = Entity.find(params[:id])
    @classifications = Classification.choices_for(@entity,'classifications')
    @classifications = @classifications - [ @entity ]
    @my_classifications = @entity.classifications
    @classifications.delete_if{|a| @my_classifications.include?(a)}
  end
  
  def update_classifications
    @entity = Entity.find(params[:id])
    @classifications = [ Classification.find(params[:classification_ids]) ] if params[:classification_ids]
    @entity.classifications << @classifications if @classifications
    redirect_to :controller => @entity.ctrlr_name, :action => 'edit', :id => @entity, :anchor => 'classifications'
  end

  def remove_from_classifications
    @entity = Entity.find(params[:id])
    @classifications = Classification.find(params[:classification_id])
    @entity.classifications.delete @classifications
    redirect_to :controller => @entity.ctrlr_name, :action => 'edit', :id => @entity, :anchor => 'classifications'
  end

  def quick_add_classifications
    @entity = Entity.find(params[:id])
    
    str_names = params[:names].strip
    if str_names.length > 0 then
      names = str_names.split("\n")
      existing_classifications = @entity.classifications.find(:all)
      for str_name in names do
        str_name.strip!
        new_obj = Classification.find_or_create_by_name(str_name)
        @entity.classifications << new_obj unless existing_classifications.include?(new_obj)
      end
    end
    
    redirect_to :action => 'edit', :id => @entity.id
  end


  def csv_upload
    # no need to prepare any data - just show the view to post against 'upload_csv'
  end

  def upload_csv

    # commented out - instead try to load the csv without an intermediate file
    #fn = "#{RAILS_ROOT}/uploads/entity_data.csv"
    #File.open(fn, "wb") { |f| f.write(params[:csv_file].read) }
    #Entity.load_csv_file(fn) if Entity.respond_to? :load_csv_file

    Entity.load_csv_str(params[:csv_file].read) if Entity.respond_to? :load_csv_str
    
    redirect_to :action => 'list' 
  end

  def download_csv
    attr_names = Entity.value_attrs

    csv_string = String.new
    csv_string = attr_names.join(",") + "\n"

    # override visual pagination establishing the limit to 100,000 rows
    params[:page] = '1'
    params[:page_length] = '100000'

    prepare_list()

    all_objs = @entities      
    
    if all_objs.size > 0 then
      all_objs.each do |record| 
        csv_string << '"'+attr_names.collect{|s| record.get_attr_val(s,'csv').to_s.gsub("\"","\"\"")}.join('","')+'"' + "\n"
      end
    end
    send_data csv_string, :filename => 'entity_data.csv', :type => 'text/csv'
  end 

  # --- 

  def show_queries
    @ft_subject = "Entity"
    @ft_activity  = "Reports"

    @queries = Entity.query_specs

    render :partial => 'fast_ops/queries', :layout => 'application'
  end

  def run_query
    @def_page_size ||= 20
    @page_size = params[:page_length].to_i > 0 ? params[:page_length].to_i : @def_page_size

    @page_no = [ params[:page].to_i, 1 ].max
    @page_clause = nil # "LIMIT #{@page_size.to_s} OFFSET #{((@page_no-1)*@page_size).to_s}"

    @query = Entity.query_specs[params[:query]]
    @sort_fields = params[:order_by] || (! @query[:order_by].to_s.empty? && @query[:order_by].join(",")) || @query[:cols][0]
    @where_clause = params[:where] || (@query[:where].to_s.length > 0 ? @query[:where] : nil)

    @base_sql = @query[:sql] + (@where_clause ? " WHERE #{@where_clause}" : "") 
    @count = Entity.count_by_sql("SELECT COUNT(id) FROM (#{@base_sql}) AS row_data")
    
    @sql = @base_sql + (@sort_fields ? " ORDER BY #{@sort_fields}" : "") + (@page_clause ? " #{@page_clause}" : "") 
    @rows = Entity.find_by_sql(apply_limit(@sql))

    @cols = @query[:cols] || (@rows.size > 0 && @rows[0].attributes.keys)

    @link_params = { :controller => 'entities', :action => 'show' }

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

    @ft_subject  = ( @ft_subject || "Entity".pluralize )
    @ft_activity = ( @ft_activity || "All" )

    @order_field = ( params[:order_by] || "name" )
    @direction = ( params[:direction] || "ASC" )

    @query_param = ( params[:query] || "list_all" )
    params[:query] = @query_param
    
    params[:order_by] = quoted_name(@order_field) + " " + @direction

    @where_param = params[:where]  # save the original where param

    adjust_sorting_and_filtering_params(Entity,params)
    @order_field = params[:sort_field].to_s unless params[:sort_field].to_s.empty?

    @entities = run_query()

    # restore original params passed to the server
    params.clear
    params.merge!(orig_params)
  end

  # --- 

#BEGIN-UID.usermethods


  def render_entities_for_classification(classification)
    if classification then
      sql_id_clause = "id in ('" + classification.entity_ids.join("','") + "')"
      
      entities = Entity.find(:all, :conditions => sql_id_clause)

      @count     = entities.size
      @page_size = @count
      @page_no   = 1

      render :xml => xml_for_entities(entities)
    else
      render :xml => {'message' => 'classification not found'}.to_xml(:root => "error")
    end
  end


  #
  # If the index is called with a classification param,
  # return the entities related to the classification
  #
  #
  #TODO - add info about the context in which methods are called
  #

  before_filter :by_classification, :only => [:index]
  def by_classification
    if params[:classification] then
      classification = Classification.find_by_name(params[:classification])
      render_entities_for_classification(classification)
      return false  # do not continue to call the normal index method
    end

    return true
  end

  #
  # If show is called with a classification param (nested entities),
  # return the entities related to the classification
  #

  before_filter :nested_resource, :only => [:show]
  def nested_resource
    if params[:classification_id]
      classification = Classification.find(params[:classification_id])
      render_entities_for_classification(classification)
      return false  # do not continue to call the normal show method
    end

    return true
  end

  #
  # Ensure that entities each have a Name and UUID
  #

  def ok_to_add_or_update?
    if params[:entity][:name].to_s.empty?
      @reason = 'The name field can not be blank.'
      return false
    end
    if params[:entity][:uuid].to_s.empty?
      @reason = 'The uuid field can not be blank.'
      return false
    end
    return true
  end


  #
  # Extention method to remove entities by their UUIDs
  # params can supply a single UUID or an array of them
  #
  # .../entities/delete_by_uuid?uuid=iewgfeiwfg_ofeiwfgiswg
  #
  # or post of
  #
  # .../entities/delete_by_uuid  with post body in xml of
  #
  # <uuids type="array"><uuid>heiwf_ofweifg</uuid> ... </uuids>
  #

  #TODO - make routes deliver UUID as part of the URL w/o being a param

  def delete_by_uuid
    params[:uuids] = [ params[:uuid] ] if params[:uuid]
    params[:uuids] = [ params[:uuids] ] if params[:uuids].instance_of?(String)

    if params[:uuids]
      for uuid in params[:uuids] do
        @entity = Entity.find_by_uuid(uuid)
        @entity.destroy if @entity
      end
    end

    respond_to do |format|
      format.html { redirect_to :back }
      format.xml  { head :ok }
    end
  end


  #
  # Override the destroy method to take either an id or UUID
  #
  # .../persistence/entities/234  HTTP DELETE
  # .../persistence/entities/ouwoetw_oyoty_oyoewr  HTTP DELETE
  #

  def destroy
    @entity = Entity.find(params[:id]) if Entity.exists?(params[:id])
    @entity = Entity.find_by_uuid(params[:id]) unless @entity
    @entity.destroy if @entity

    respond_to do |format|
      format.html { redirect_to :back }
      format.xml  { head :ok }
    end
  end

  #
  # Override to add test for the type of param being a string (gsub
  # not a method for Number types)
  #

  def sanitize_params(hk)
    return unless params[hk]
    params[hk].each{|k,v| params[hk][k] = v.gsub("-- select --","") if v.instance_of?(String)}  if params[hk]
    params[hk].each{|k,v| params[hk][k] = v.gsub(/HH:MM.*/,"") if v.instance_of?(String)}       if params[hk]
  end

  #
  # Called before show, edit methods
  # Overridden to allow find by UUID as well as id
  #

  def set_context(load_related=true)
    @entity = Entity.find(params[:id]) if Entity.exists?(params[:id])
    @entity = Entity.find_by_uuid(params[:id]) unless @entity

    @ft_subject = "Entity"
    @ft_activity = @entity.iname

    load_related = false if (params[:action] == "show_properties")    # don't load related items if only showing properties
    return unless load_related

    @order_field = get_order_field(params,'entity','associations','name')
    @id_set = @entity.send("#{'associations'.singularize}_ids").concat(["0"]).join(",")
    @associations = EntityAssn.find_by_sql(%Q~select * from #{EntityAssn.default_query("entity_assn.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

    @order_field = get_order_field(params,'entity','attrs','name')
    @id_set = @entity.send("#{'attrs'.singularize}_ids").concat(["0"]).join(",")
    @attrs = EntityAttr.find_by_sql(%Q~select * from #{EntityAttr.default_query("entity_attr.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

    @order_field = get_order_field(params,'entity','member_associations','name')
    @id_set = @entity.send("#{'member_associations'.singularize}_ids").concat(["0"]).join(",")
    @member_associations = EntityAssn.find_by_sql(%Q~select * from #{EntityAssn.default_query("entity_assn.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

    @order_field = get_order_field(params,'entity','classifications','name')
    @id_set = @entity.send("#{'classifications'.singularize}_ids").concat(["0"]).join(",")
    @classifications = Classification.find_by_sql(%Q~select * from #{Classification.default_query("classification.id in (#{@id_set})")} order by #{quoted_name(@order_field)}~)

  end

  #
  # Called whenever the REST url is .../persistence/entities
  # Test what type of request is being made and re-direct accordingly
  #

  def index

    if request.post?
      create()

    elsif request.put?
      update()

    else
      params[:page_length] = Entity.count().to_s if params[:page_length] == '0'

      prepare_list()

      respond_to do |format|

        format.html {
          render :action => @ft_custom_action and return if @ft_custom_action
          render :action => 'list'
        }

        format.xml  {
          xml_text = xml_for_entities(@entities)
          render :xml => xml_text
        }
      end
    end

  end

  #
  # Override to provide ability to return entities updated since
  # a specified unix timestamp
  #

  def run_query
    @def_page_size ||= 20
    @page_size = params[:page_length].to_i > 0 ? params[:page_length].to_i : @def_page_size

    @page_no = [ params[:page].to_i, 1 ].max
    @page_clause = nil # "LIMIT #{@page_size.to_s} OFFSET #{((@page_no-1)*@page_size).to_s}"

    @query = Entity.query_specs[params[:query]]
    @sort_fields = params[:order_by] || (! @query[:order_by].to_s.empty? && @query[:order_by].join(",")) || @query[:cols][0]
    @where_clause = params[:where] || (@query[:where].to_s.length > 0 ? @query[:where] : nil)

    @base_sql = @query[:sql] + (@where_clause ? " WHERE #{@where_clause}" : "")
    @count = Entity.count_by_sql("SELECT COUNT(id) FROM (#{@base_sql}) AS row_data")

    @sql = @base_sql + (@sort_fields ? " ORDER BY #{@sort_fields}" : "") + (@page_clause ? " #{@page_clause}" : "")
    @rows = Entity.find_by_sql(apply_limit(@sql))

    @cols = @query[:cols] || (@rows.size > 0 && @rows[0].attributes.keys)

    @link_params = { :controller => 'entities', :action => 'show' }

    return @rows
  end

  #
  # Override to allow for the creation of multiple entities in a
  # single post
  #

  def create
    unless params[:entities] || params[:entity] then
      render :xml => { :message => "Missing or invalid Entity XML content in POST paramaters"}.to_xml(:root => "error")
      return
    end

    if params[:entities] then
      create_many()
    else
      create_one(true)
    end
  end

#  def process_batch?
#    # remember group names we have seen
#    $loaded_group_names = {} if ! defined?($loaded_group_names) || $loaded_group_names.nil?
#
#    # only load this batch if it contains some new group
#    do_batch = false
#    for entity in params[:entities] do
#      return true if entity[:creation_class].to_s["Manager"] || entity[:creation_class].to_s["Setting"]
#
#      # if unknown entity name or info is in need of refresh...
#      n_hours = 12
#      if $loaded_group_names[entity[:name]].nil? || (Time.now - $loaded_group_names[entity[:name]]) > n_hours*60*60
#        do_batch = true
#        $loaded_group_names[entity[:name]] = Time.now
#      end
#    end
#
#    logger.error("*** Ignoring batch of redundant group information") unless do_batch
#
#    return do_batch
#  end

  #
  # When creating many, return instantly to the client after
  # launching a batch operation to load the entities
  #

  def create_many

#    unless process_batch?
#      head :ok
#      return
#    end

    # defer loading to a separate process

    dir = "#{RAILS_ROOT}/doc_queue"
    FileUtils.mkdir dir, :mode => 0777 unless File.exist?(dir)

    file_name = "batch_"+(Time.now.to_f*1000).to_i.to_s+".xml"
    file_path = dir + "/" + file_name

    File.open(file_path, 'w') { |f| f.write(request.raw_post) }
    f = File.new(file_path); f.chmod 0766; f.close()

    # ---

    split_cmd = "ruby "+RAILS_ROOT+"/script/entity_splitter.rb -f "+file_path
    split_cmd = split_cmd.gsub(/^ruby/,"jruby -S") if RUBY_PLATFORM["java"]

    system(split_cmd)

    # ---

    load_cmd  = "ruby "+RAILS_ROOT+"/script/runner -e production "+RAILS_ROOT+"/script/entity_loader.rb -f "+file_path
    load_cmd = load_cmd.gsub(/^ruby/,"jruby -S") if RUBY_PLATFORM["java"]

    unless $loader_spawned_at && (Time.now-$loader_spawned_at) < 10
      $loader_spawned_at = Time.now
      if RUBY_PLATFORM["win32"] || RUBY_PLATFORM["win64"] ||
         RUBY_PLATFORM["mingw32"] || RUBY_PLATFORM["mingw64"] || RUBY_PLATFORM["java"] then

        system("start /B " + load_cmd)

      else

        system("bash "+RAILS_ROOT+"/script/run_loader.sh")

      end
    end


    head :ok

  end


  META_GROUPS = [
    "GROUP-AllVirtualMachine",
    "GROUP-Application",
    "GROUP-DataCenter",
    "GROUP-MyGroups",
    "GROUP-OtherGroups",
    "GROUP-PhysicalMachine",
    "GROUP-PMGroups",
    "GROUP-Storage",
    "GROUP-VirtualMachine",
    "GROUP-VMGroups",
    "GROUP-PhysicalMachineByCluster",
    "GROUP-PhysicalMachineByDataCenter",
    "GROUP-PM_Parent",
    "GROUP-VirtualMachineByCluster",
    "GROUP-VirtualMachineByDataCenter",
    "GROUP-VirtualMachineByFolder",
    "GROUP-VirtualMachineByNetwork",
    "GROUP-VirtualMachineByPhysicalMachine",
    "GROUP-VirtualMachineByStorage",
    "GROUP-VCImportedGroups",
    ]

  #
  # Serves both HTML and REST clients; HTML redirects to a view, but
  # REST clients get back a head :ok or error in XML
  #

  def create_one(render_xml_response)

    sanitize_params(:entity)

    @entity_params = { }
    @entity_params[:name] = params[:entity][:name]
    @entity_params[:display_name] = params[:entity][:display_name]
    @entity_params[:uuid] = params[:entity][:uuid]
    @entity_params[:creation_class] = params[:entity][:creation_class]

    @entity_params[:display_name] = @entity_params[:name] if @entity_params[:display_name].to_s.empty?

    #TODO - make queries use the Group classification instead of creation class
    @entity_params[:creation_class] = "Group" if @entity_params[:creation_class] == "Folder"


    # UUIDs for groups are not durable.
    # Get rid of old groups having this name but differing UUIDs

    if ["Group","MetaGroup"].include?(@entity_params[:creation_class].to_s)
      Entity.destroy_all("name = '#{@entity_params[:name]}' AND creation_class in ('Group','MetaGroup') and uuid <> '#{@entity_params[:uuid]}'")
    end

    if META_GROUPS.include?(@entity_params[:name]) ||
         @entity_params[:name].to_s.include?(", ") then

      @entity_params[:creation_class] = "MetaGroup"
    end


    unless params[:entity][:uuid].to_s.empty?
      if @entity = Entity.find(:first, :conditions => "uuid = '#{params[:entity][:uuid]}'")
        @entity.attributes = @entity_params
      else
        @entity = Entity.new(@entity_params)
      end
    else
      @entity = Entity.new(@entity_params)
    end

    @new_obj = @entity  # keep - @new_obj may be used in generalized 'create' aspect handlers

    if ! ok_to_add_or_update?
        flash[:notice] = 'Not added. ' + @reason.to_s
        @ft_subject = "Entity"
        @ft_activity = "New"
        render :action => 'new'
    else
      respond_to do |format|
        if @entity.save
          create_or_update_nested_items(params[:entity])

          entity_cache_put(@entity)

          format.xml {
            head :ok if render_xml_response
          }

          format.html {
            flash[:notice] = "#{@entity.iname} was successfully created."
            if session[:post_create_redirect]
                url = session[:post_create_redirect] + "?entity_id=" + @entity.id.to_s + "&entity_ids[]=" + @entity.id.to_s
                redirect_to url
                session[:post_create_redirect] = nil
            else
                redirect_to :id => @entity.id, :action => (params[:commit].to_s.include?("Another") ? 'new' : 'edit')
            end
          }

        else
          format.xml  {
            render(:xml => @entity.errors, :status => :unprocessable_entity) if render_xml_response
          }

          format.html {
            flash[:notice] = 'Entity could not be created. See errors.'
            render :action => 'new'
          }

        end
      end
    end
  end


  #
  # Overridde to allow for use of ID or UUID identity and
  # to handle nested items that are updated with the entity
  #

  def update
    unless params[:entities] || params[:entity] then
      render :xml => { :message => "Missing or invalid Entity XML content in PUT paramaters"}.to_xml(:root => "error")
      return
    end

    sanitize_params(:entity)

    @entity = Entity.find(params[:id]) if Entity.exists?(params[:id])
    @entity = Entity.find_by_uuid(params[:id]) unless @entity
    @entity = Entity.find_or_create_by_uuid(params[:id]) unless @entity

    @updated_obj = @entity

    params[:name] = "not-set" if params[:name].to_s.empty?

    respond_to do |format|
      @entity_params = params[:entity].clone
      @entity_params.delete_if {|k,v| ['id','uuid'].include?(k) } # ids and uuids are not changed once set
      @entity_params.delete_if {|k,v| ['entity_attrs','entity_assns','classifications'].include?(k) }

      if @entity.update_attributes(@entity_params)
        create_or_update_nested_items(params[:entity])

        entity_cache_put(@entity)

        format.xml  {
          head :ok
        }

        format.html {
          flash[:notice] = 'Entity was successfully updated.'
          redirect_to :action => 'show', :id => @entity
        }
      else
        format.xml  { render :xml => @entity.errors, :status => :unprocessable_entity }
        format.html {
          redirect_to :action => 'edit', :id => @entity
        }
      end
    end
  end


  #
  # Adds the nested items from the XML received to the entity
  # Default mode is to use the input XML as definitive and overwrite
  # the contents of the entity
  #

  def create_or_update_nested_items(entity_xml)
    overwrite = (entity_xml[:overwrite] || "true") == "true"


    classifications_arr = entity_xml[:classifications] || []
    @entity.classifications.clear if overwrite && entity_xml[:classifications]

    c_ids = @entity.classification_ids

    for cl in classifications_arr do
      ccl = Classification.find_or_create_by_name(cl[:name])
      unless c_ids.include?(ccl.id)
        @entity.classifications << ccl
        c_ids << ccl.id
      end
    end

    unless entity_xml[:creation_class].to_s.empty?
      ccl = Classification.find_or_create_by_name(entity_xml[:creation_class])
      unless c_ids.include?(ccl.id)
        @entity.classifications << ccl
        c_ids << ccl.id
      end
    end


    attrs_arr = entity_xml[:entity_attrs] || []
    @entity.attrs.destroy_all if overwrite && entity_xml[:entity_attrs]

    for attr in attrs_arr do
      attribute_attrs = { :name => attr[:name], :value => attr[:value] }

      next if attribute_attrs[:value].to_s.empty?

      if attribute_attrs[:value].instance_of?(Array) then
        attribute_attrs[:multi_values] = attribute_attrs[:value].join(",")
        attribute_attrs[:value] = nil
      elsif attribute_attrs[:value].to_s.include?(",")
        attribute_attrs[:multi_values] = attribute_attrs[:value].to_s
        attribute_attrs[:value] = nil
      else
        attribute_attrs[:multi_values] = nil
      end

      if attr_obj = @entity.attrs.find(:first, :conditions => "name = '#{attribute_attrs[:name]}'")
        attr_obj.update_attributes(attribute_attrs)
      else
        @entity.attrs.create(attribute_attrs)
      end
    end


    assns_arr = entity_xml[:entity_assns] || []
    @entity.associations.destroy_all if overwrite && entity_xml[:entity_assns]

    for assn in assns_arr do
      unless assn_obj = @entity.associations.find(:first, :conditions => "name = '#{assn[:name]}'")
        assn_obj = @entity.associations.create(:name => assn[:name])
      end

      m_ids = assn_obj.member_ids
      members_arr = assn[:members] || []
      for member in members_arr do
        # create the member if it is not there
        unless ref_obj = Entity.find(:first, :conditions => "uuid = '#{member[:uuid]}'")
          ref_obj = Entity.create(:name => member[:uuid], :uuid => member[:uuid])
        end
        assn_obj.members << ref_obj unless m_ids.include?(ref_obj.id)
      end
    end

  end

  #
  # Override to use the entity XML cache
  #

  def show
    set_context()

    respond_to do |format|

      format.html {
        render :action => @ft_custom_action if @ft_custom_action
      }

      format.xml  {
        unless xml_body = entity_cache_get(@entity.uuid)
          xml_body = xml_for_entity(@entity,true)
          entity_cache_put_xml(@entity.uuid,xml_body)
        end

        render :xml => '<?xml version="1.0" encoding="UTF-8"?>' + "\n" + xml_body
      }

    end
  end


  #
  # Method to get the associated entities of the current entity by the
  # name of the association
  #

  def get_related
    set_context()

    respond_to do |format|

      assn = @entity.associations.find_by_name(params[:relname])
      @entities = assn ? assn.members.find(:all) : []

      format.xml {
        render :xml => xml_for_entities(@entities)
        return
      }

      format.html {
        @query = Entity.query_specs()['list_all']
        @rows = @entities
        @cols = ['name','created_at']
        @link_params = { :controller => 'entities', :action => 'show' }
        render :partial => 'fast_ops/query_frame', :layout => 'application'
      }
    end
  end

  #
  # Return one or more entities by the provided UUID(s)
  #

  def fetch_by_uuid
    xml_to_return = '<?xml version="1.0" encoding="UTF-8"?>'+"\n"
    xml_to_return << '<entities type="array">'+"\n"

    params[:uuids] = [ params[:uuid] ] if params[:uuid]
    params[:uuids] = [ params[:uuids] ] if params[:uuids].instance_of?(String)

    for uuid in params[:uuids] || [] do
      unless e_xml = entity_cache_get(uuid)
        if entity = Entity.find(:first, :conditions => "uuid = '#{uuid}'")
          e_xml = xml_for_entity(entity, true)
          entity_cache_put_xml(uuid,e_xml)
        end
      end

      unless e_xml.nil?
        xml_to_return << e_xml + "\n"
      end
    end

    xml_to_return << '</entities>'+"\n"

    render :xml => xml_to_return
  end

  #
  # Implementation equivalent of Shai's "getValues" method
  #

  def get_values
    unless params[:get_values]
      render :xml => {:error => {:message => "Missing expected post data"}}.to_xml
      return
    end

    req_info = params[:get_values]

    uuids = req_info[:uuids].compact
    instructions = req_info[:elements].compact || []

    entities = []
    entities = Entity.find(:all, :conditions => "uuid in ('#{uuids.join("','")}')") if uuids.size > 0

    collected_values = []

    attrs_rendered = false

    for step in instructions do
      if attrs_rendered then
        entities = collected_values = []
        break
      end

      name = step[:name]

      case step[:kind]
        when "reference"
          collected_entities = []
          for entity in entities do
            if related_items = entity.associations.find(:first, :conditions => "name = '#{name}'") then
              collected_entities = collected_entities.concat(related_items.members.find(:all))
            end
          end
          entities = collected_entities.uniq

        when "class"
          if cl = Classification.find(:first, :conditions => "name = '#{name}'")
            entities = entities.delete_if{|entity| ! entity.classification_ids.include?(cl.id)}
          else
            entities = []  # no such class - so no members match to return
          end

        when "attribute"  # must be the last instruction, so return the results
          for entity in entities do
            if Entity.value_attrs.include?(name.gsub("-","_")) then
              collected_values << entity.send(name)
            else
              if attr = entity.attrs.find(:first, :conditions => "name = '#{name}'") then
                collected_values << attr.value unless attr.value.to_s.empty?
                collected_values = collected_values.concat(attr.multi_values.split(",")) unless attr.multi_values.to_s.empty?
              end
            end
          end
          collected_values.uniq!
          attrs_rendered = true
      end
    end

    if attrs_rendered
      @count = collected_values.size
      @page_size = @count
      @page_no = 1

      render :xml => values_as_xml(collected_values)
    else
      @count = entities.size
      @page_size = @count
      @page_no = 1

      render :xml => xml_for_entities(entities)
    end
  end

  #
  # Helper method to return the XML for a set of values
  #

  def values_as_xml(collected_values)
    xml_attrs = { :type => 'array',
              :page => @page_no,
              :page_length => @page_size,
              :number_returned => collected_values.size,
              :number_available => @count }

    xml = Builder::XmlMarkup.new(:indent => 2)
    xml.instruct!
    xml.values(xml_attrs) do
      for val in collected_values do
        xml.value val
      end
    end
  end


  #
  # Helper method to return the XML representation of a single entity
  # either for use in an array (no_header) or for returning as a
  # single entity
  #

  def xml_for_entity(entity, no_header)
    xml_str = entity.to_xml({:skip_instruct => no_header})
    return xml_str
  end


  #
  # Helper method to return the XML representation of an
  # array of entities
  #

  def xml_for_entities(entities)
    attrs = { :type => 'array',
              :page => @page_no,
              :page_length => @page_size,
              :number_returned => entities.size,
              :number_available => @count }

    xml = Builder::XmlMarkup.new(:indent => 2)
    xml.instruct!
    xml.entities(attrs) do
      entities.each{|e| e.to_xml(:builder => xml, :skip_instruct => true)}
    end
  end

#END-UID.usermethods

end

