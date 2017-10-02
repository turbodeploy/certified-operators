
require 'topology_svc'

class MobiController < ApplicationController

  before_filter :login_required


  ECODE_FOR  = {'PhysicalMachine' => 'pm', 'VirtualMachine' => 'vm', 'Storage' => 'ds'}
  CLASS_NAME = {'pm' => 'Host', 'vm' => 'Virtual Machine', 'ds' => 'Datastore'}


  def get_problems
    return $probs if session[:index_data_loaded] && $probs
    $probs = []

    svc = TopologyService.new

    mkt_uuid        = svc.get_related_uuids("MarketManager", "MainMarket").first
    act_log_uuid    = svc.get_related_uuids(mkt_uuid, "actionLog").first

    ai_hash_arr = []
    act_item_uuids  = svc.get_related_uuids(act_log_uuid, "items")
    for uuid in act_item_uuids do
      xml = svc.xml_for(uuid, "name")
      ai_hash_arr << svc.attrs_for(xml)
    end

    keep_attrs = %W(explanation class_name obj_name obj_display_name obj_uuid problem_name severity)

    for ai_hash in ai_hash_arr do
      ai_rels_xml   = svc.xml_for(ai_hash['uuid'],".*")
      ai_attrs      = svc.attrs_for(ai_rels_xml)

      explanation   = ai_attrs['explanation']
      rel_uuids     = svc.get_related_uuids(ai_hash['uuid'],"relatedProblem")
      prob_attrs    = svc.attrs_for(svc.xml_for(rel_uuids.first, "severity"))

      next unless prob_attrs['displayName']
      klass, mob, prob = prob_attrs['displayName'].split("::")

      obj_attrs = svc.attrs_for(svc.xml_for(mob))

      prob_attrs['explanation']       = explanation
      prob_attrs['class_name']        = klass
      prob_attrs['obj_name']          = mob
      prob_attrs['obj_display_name']  = obj_attrs['displayName']
      prob_attrs['obj_uuid']          = obj_attrs['uuid']
      prob_attrs['problem_name']      = prob

      #puts prob_attrs.inspect

      prob_attrs.delete_if{|k,v| ! keep_attrs.include?(k)}

      $probs << prob_attrs
    end

    return $probs
  end



  # influences the sort order to impose grouping of props
  def prefix(prop)
    case
    when ['Swapping','Ballooning'].include?(prop)
      return "x"
    when prop["Throughput"]
      return "y"
    when prop[/Q\d\d/]
      return "Qz"
    else
      return ""
    end
  end

  def index
    @high_mem = 0.80
    @high_cpu = 0.60

    @problems = get_problems
    @pm_problems = @problems.select{|p| p['class_name'] == 'PhysicalMachine'}
    @vm_problems = @problems.select{|p| p['class_name'] == 'VirtualMachine'}
    @ds_problems = @problems.select{|p| p['class_name'] == 'Storage'}

    unless session[:index_data_loaded] && $pm_entity_info
      $pm_entity_info = @pm_entity_info = Entity.find_by_sql("select * from pm_util_stats_yesterday")
      $vm_entity_info = @vm_entity_info = Entity.find_by_sql("select * from vm_util_stats_yesterday")
      $ds_entity_info = @ds_entity_info = Entity.find_by_sql("select * from ds_util_stats_yesterday")

      $pm_props = @pm_props = @pm_entity_info.collect{|e| e.property_type}.sort.uniq.sort{|a,b| prefix(a)+a <=> prefix(b)+b}
      $vm_props = @vm_props = @vm_entity_info.collect{|e| e.property_type}.sort.uniq.sort{|a,b| prefix(a)+a <=> prefix(b)+b}
      $ds_props = @ds_props = @ds_entity_info.collect{|e| e.property_type}.sort.uniq.sort{|a,b| prefix(a)+a <=> prefix(b)+b}

    else
      @pm_entity_info = $pm_entity_info
      @vm_entity_info = $vm_entity_info
      @ds_entity_info = $ds_entity_info

      @pm_props = $pm_props
      @vm_props = $vm_props
      @ds_props = $ds_props
    end

    session[:index_data_loaded] = true
  end

  def refresh
    session[:index_data_loaded] = nil
    redirect_to :action => 'index'
  end

  # # #

  def show_problems
    @class_name = params[:klass].to_s
    @e_code = ECODE_FOR[@class_name]
    @problems = get_problems
    @problems = @problems.select{|p| p['class_name'] == @class_name}
  end

  # # #

  def top_10_hosts
    n_vals = params[:n_vals].to_s.to_i
    run_top_n_query('pm',[10,n_vals].max)
    render :action => 'entity_list'
  end

  def top_10_vms
    n_vals = params[:n_vals].to_s.to_i
    run_top_n_query('vm',[10,n_vals].max)
    render :action => 'entity_list'
  end

  def top_10_dss
    n_vals = params[:n_vals].to_s.to_i
    run_top_n_query('ds',[10,n_vals].max)
    render :action => 'entity_list'
  end

  # # #

  def bottom_10_hosts
    n_vals = params[:n_vals].to_s.to_i
    run_bottom_n_query('pm',[10,n_vals].max)
    render :action => 'entity_list'
  end

  def bottom_10_vms
    n_vals = params[:n_vals].to_s.to_i
    run_bottom_n_query('vm',[10,n_vals].max)
    render :action => 'entity_list'
  end

  def bottom_10_dss
    n_vals = params[:n_vals].to_s.to_i
    run_bottom_n_query('ds',[10,n_vals].max)
    render :action => 'entity_list'
  end

  # # #

  def hot_spots
  end

  def wasted_resources
  end

  def inventory
    @clusters = Entity.find_by_sql("select * from pm_groups").sort{|a,b| a.group_name.gsub("PMs_","") <=> b.group_name.gsub("PMs_","")}
    @hosts    = Entity.find_by_sql("select distinct display_name, uuid from entities where creation_class = 'PhysicalMachine' order by display_name")
  end

  # # #
  
  def summary_cluster
    @group_name = params[:group_name].to_s.gsub("'","''").gsub("\\","\\\\\\\\")

    @stats = Entity.find_by_sql(%Q~select property_type, property_subtype, avg(avg_value) as property_value
              from
                (
                  select *
                  from
                    (select * from pm_group_members where group_name = '#{@group_name}') as t1,
                    (select * from pm_stats_by_day where date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)) as t2
                  where
                    member_uuid = uuid
                ) as data
              where
                property_subtype = 'utilization'
              group by
                property_type, property_subtype
      ~.gsub("\n"," "))

    @stats_by_name = Entity.find_by_sql(%Q~select display_name, uuid, property_type, property_subtype, avg_value as property_value
              from
                (
                  select *
                  from
                    (select * from pm_group_members where group_name = '#{@group_name}') as t1,
                    (select * from pm_stats_by_day where date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)) as t2
                  where
                    member_uuid = uuid
                ) as data
              where
                property_subtype = 'utilization'
      ~.gsub("\n"," "))

    @grouped_stats = {}
    for s in @stats_by_name do
      @grouped_stats[s.uuid] = {} unless @grouped_stats[s.uuid]
      @grouped_stats[s.uuid]['display_name'] = s.display_name
      @grouped_stats[s.uuid][s.property_type] = s.property_value
    end

  end


  def summary_pm
    @pm_uuid = params[:pm_uuid]

    @stats = PmStatsByDay.find_by_sql(%Q~select display_name, entities.uuid as uuid, property_type,
                                          property_subtype, avg_value as property_value, max_value as max_property_value
          from (
          select * from pm_stats_by_day
            where
              date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)
              and uuid = '#{@pm_uuid}'
              and property_subtype = 'utilization'
            ) as t1, entities
          where
            max_value >= 0.00
            and t1.uuid = entities.uuid
      ~.gsub(/\s+/," "))

    @pm_name = @stats.first ? @stats.first.display_name : "No longer in inventory"

    @vm_stats = VmStatsByDay.find_by_sql(%Q~select display_name, uuid, property_type,
                                            property_subtype, avg_value as property_value
              from
                (
                  select *
                  from
                    (select * from vm_group_members where group_name = 'VMs_#{@pm_name}') as t1,
                    (select * from vm_stats_by_day where date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)) as t2
                  where
                    member_uuid = uuid
                ) as data
              where
                property_subtype = 'utilization'
      ~.gsub("\n"," "))

    @grouped_stats = {}
    for s in @vm_stats do
      @grouped_stats[s.uuid] = {} unless @grouped_stats[s.uuid]
      @grouped_stats[s.uuid]['display_name'] = s.display_name
      @grouped_stats[s.uuid][s.property_type] = s.property_value
    end

  end

  
  def summary_vm
    @vm_uuid = params[:vm_uuid]

    @stats = PmStatsByDay.find_by_sql(%Q~select display_name, entities.uuid as uuid, property_type,
                                          property_subtype, avg_value as property_value, max_value as max_property_value
          from (
          select * from vm_stats_by_day
            where
              date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)
              and uuid = '#{@vm_uuid}'
              and property_subtype = 'utilization'
            ) as t1, entities
          where
            max_value >= 0.00
            and t1.uuid = entities.uuid
      ~.gsub(/\s+/," "))

    @vm_name = @stats.first ? @stats.first.display_name : "No longer in inventory"

    @stats_used = VmStatsByDay.find_by_sql(%Q~select display_name, entities.uuid as uuid, producer_uuid, property_type,
                                              property_subtype, (avg_value) as property_value, (max_value) as max_property_value
              from (select * from vm_stats_by_day
                where date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)
                  and uuid = '#{@vm_uuid}'
                  and property_subtype in ('used','swap','log','snapshot','disk','unused')) as t1,
                entities
                  where t1.uuid = entities.uuid
      ~.gsub("\n"," "))
  end

  def summary_ds
    @ds_uuid = params[:ds_uuid]

    @stats = DsStatsByDay.find_by_sql(%Q~select display_name, entities.uuid as uuid, property_type,
                                          property_subtype, capacity, avg_value as property_value, max_value as max_property_value
          from (
          select * from ds_stats_by_day
            where
              date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)
              and uuid = '#{@ds_uuid}'
              and property_subtype = 'utilization'
            ) as t1, entities
          where
            max_value >= 0.00
            and t1.uuid = entities.uuid
      ~.gsub(/\s+/," "))

    @ds_name = @stats.first ? @stats.first.display_name : "No longer in inventory"

    @vm_stats = VmStatsByDay.find_by_sql(%Q~select display_name, uuid, property_type,
                                            property_subtype, avg_value as property_value
              from
                (
                  select *
                  from
                    (select * from vm_group_members where group_name = 'VMs_#{@ds_name}') as t1,
                    (select * from vm_stats_by_day where date(from_unixtime(snapshot_time/1000)) = date_sub(date(now()), interval 1 day)) as t2
                  where
                    member_uuid = uuid
                ) as data
              where
                property_type in ('StorageAmount','StorageAccess','StorageLatency')
                and property_subtype = 'used'
      ~.gsub("\n"," "))

    @grouped_stats = {}
    for s in @vm_stats do
      @grouped_stats[s.uuid] = {} unless @grouped_stats[s.uuid]
      @grouped_stats[s.uuid]['display_name'] = s.display_name
      @grouped_stats[s.uuid][prop_name(s.property_type)] = s.property_value
    end
  end

  def prop_name(db_name)
    case db_name
    when 'StorageAccess'
      return 'IOPS'
    when 'StorageLatency'
      return 'Latency(MS)'
    when 'StorageAmount'
      return 'Used(MB)'
    else
      return db_name
    end
  end

  # # #

  def run_top_n_query(e_code, n_vals = 10)
    @prop_type     = params[:prop_type].to_s
    @prop_subtype  = params[:prop_subtype].to_s
    @cutoff        = params[:cutoff] ? params[:cutoff].to_f : 0.50

    @n_days        = params[:n_days] ? params[:n_days].to_i : 1

    @page          = params[:page] ? params[:page].to_i : 1

    @direction     = params[:direction] ? params[:direction] : "DESC"

    @e_code = e_code
    @obj_type = CLASS_NAME[@e_code]
    @list_title = "Resource: #{@prop_type}"


    @prefix = ""
    @suffix = @prop_subtype == "utilization" ? "%" : ""
    @factor = @prop_subtype == "utilization" ? 100.0 : 1.0

    @entities = []

    unless @prop_type.empty? || @prop_subtype.empty?
      sql = <<-STR
              select entities.id as id, entities.uuid as uuid, entities.display_name as display_name,
                creation_class, max_value as value
              from #{e_code}_stats_by_day, entities
              where date(from_unixtime(snapshot_time/1000)) >= date_sub(date(now()), interval #{@n_days} day)
                and max_value >= #{@cutoff}
                and property_type = '#{@prop_type}'
                and property_subtype = '#{@prop_subtype}'
                and entities.uuid = #{e_code}_stats_by_day.uuid
              group by uuid, property_type, property_subtype
              order by max_value #{@direction}
              limit #{(n_vals+1).to_s}
              offset #{((@page-1)*n_vals).to_s}
            STR
      sql = sql.gsub(/\s+/,' ')

      @entities = Entity.find_by_sql(sql)
    end
  end

  # # #

  def run_bottom_n_query(e_code, n_vals = 10)
    @prop_type     = params[:prop_type].to_s
    @prop_subtype  = params[:prop_subtype].to_s
    @cutoff        = params[:cutoff] ? params[:cutoff].to_f : 0.20

    @n_days        = params[:n_days] ? params[:n_days].to_i : 1
    @low_val       = params[:low_val] ? params[:low_val].to_f : 0.0

    @page          = params[:page] ? params[:page].to_i : 1

    @direction     = params[:direction] ? params[:direction] : "ASC"

    @e_code = e_code
    @obj_type = CLASS_NAME[@e_code]
    @list_title = "Resource: #{@prop_type}"


    @prefix = ""
    @suffix = @prop_subtype == "utilization" ? "%" : ""
    @factor = @prop_subtype == "utilization" ? 100.0 : 1.0

    @entities = []

    unless @prop_type.empty? || @prop_subtype.empty?
      sql = <<-STR
              select entities.id as id, entities.uuid as uuid, entities.display_name as display_name,
                creation_class, max_value as value
              from #{e_code}_stats_by_day, entities
              where date(from_unixtime(snapshot_time/1000)) >= date_sub(date(now()), interval #{@n_days} day)
                and max_value >= #{@low_val} and max_value < #{@cutoff}
                and property_type = '#{@prop_type}'
                and property_subtype = '#{@prop_subtype}'
                and entities.uuid = #{e_code}_stats_by_day.uuid
              group by uuid, property_type, property_subtype
              order by max_value #{@direction}
              limit #{(n_vals+1).to_s}
              offset #{((@page-1)*n_vals).to_s}
            STR
      sql = sql.gsub(/\s+/,' ')

      @entities = Entity.find_by_sql(sql)
    end
  end

end
