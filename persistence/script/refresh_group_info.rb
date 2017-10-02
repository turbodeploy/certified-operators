
require 'topology_svc'
require 'api_svc'

$top_svc = TopologyService.new
$api_svc = ApiService.new


$GROUP_CLASSES = ["Folder","Pool","VPool","Group", "Cluster", "StorageCluster", "VirtualDataCenter"]
$ALL_GRP_MEM_GRP_CLSS = ["Folder","Pool","VPool", "Cluster", "StorageCluster", "VirtualDataCenter"]
$SE_CLASSES = ['PhysicalMachine', 'VirtualMachine', 'Storage', 'DataCenter', 'VirtualDataCenter']


TopologyItem = Struct.new(:class_name, :name, :display_name, :uuid, :report_enabled, :group_stats_enabled)

GroupStructure = Struct.new(:group_item, :members, :scope_type, :group_name, :group_type, :report_enabled, :group_stats_enabled)

# # #

def make_item(uuid)
  obj_xml   = $top_svc.xml_for(uuid,'reportEnabled|groupStatsEnabled')
  obj_attrs = $top_svc.attrs_for(obj_xml)

  item = TopologyItem.new(obj_attrs['creationClassName'],
                          obj_attrs['name'],
                          obj_attrs['displayName'],
                          obj_attrs['uuid'],
                          obj_attrs['reportEnabled'],
                          obj_attrs['groupStatsEnabled']  )
  return item
end

def get_members(top_items, rel_name)
  members = []

  top_items = [ top_items ].flatten
  for parent_item in top_items do
    related_item_uuids = $top_svc.get_related_uuids(parent_item.uuid,rel_name)
    for uuid in related_item_uuids do
      members << make_item(uuid)
    end
  end

  return members
end

def print_group_info(grp_struct)
  puts
  puts "=" * 80 + " " + grp_struct.scope_type + ", " + grp_struct.group_name + ", " + grp_struct.group_type
  puts grp_struct.group_item.inspect

  for member in grp_struct.members do
    print "   "
    puts member.inspect
  end
end

def print_groups(groups)
  for g in groups do
    print_group_info(g)
  end
end

# # #

def groups_for(meta_group, scope)
  arr = [ ]

  uuids = $top_svc.get_instance_uuids(meta_group)
  puts "*** No members for #{meta_group}" if uuids.empty?
  for uuid in uuids do
    make_grp_and_members(meta_group, scope, uuid, arr)
  end

  return arr
end

def meta_group_gen_members_for(meta_group)
  arr = [ ]
  
  grp_xml = $top_svc.xml_for("GROUP-#{meta_group}")
  grp_attrs = $top_svc.attrs_for(grp_xml)
  return arr if !grp_attrs.has_key?('uuid')
  
  #puts attrs.inspect
  uuids = [grp_attrs['uuid']]
  puts "*** No such group GROUP-#{meta_group}" if uuids.empty?
  for uuid in uuids do
    make_grp_and_members(meta_group, "", uuid, arr, "AllGenConsistsOf")
  end

  return arr
end

def make_grp_and_members(grp_name, scope, uuid, arr, members_ref="AllConsistsOf")
  grp_item = make_item(uuid)
  grp_members = get_members(grp_item, members_ref)
  return if grp_members.empty?

  case
  when grp_name["VirtualMachineBy"]
    group_class = "VirtualMachine"
  when grp_name["PhysicalMachineBy"]
    group_class = "PhysicalMachine"
  else
    group_class = grp_members.first.class_name
  end

  group_name = grp_item.display_name
  arr << GroupStructure.new(grp_item, grp_members, scope, group_name, group_class, grp_item.report_enabled, grp_item.group_stats_enabled)

  if ["DataCenter","MyGroup"].include?(scope) && group_class == "PhysicalMachine"
    display_name = "VMs_"+grp_item.display_name
    display_name = display_name.gsub("VMs_PMs_","VMs_")
    vm_grp_item = TopologyItem.new("Group",grp_item.name+"-vms",display_name,grp_item.uuid+"-vms")
    vm_grp_members = get_members(grp_members, "Hosts")
    arr << GroupStructure.new(vm_grp_item, vm_grp_members, scope, vm_grp_item.display_name, "VirtualMachine", grp_item.report_enabled, grp_item.group_stats_enabled)
  end
end

def get_se_subgroups(grp_item,parent=nil)
  map = get_se_subgroups_map(grp_item, parent)
  return map.values
end

def get_se_subgroups_map(grp_item,parent=nil)
  return {} if grp_item.nil?

  map = {}

  query_prop = "ConsistsOf"
  query_prop = "AllConsistsOf" if $ALL_GRP_MEM_GRP_CLSS.include?(grp_item.class_name)


  grp_members = get_members(grp_item, query_prop).compact
  return {} if grp_members.empty?

  grp_item.display_name = grp_item.class_name=="Cluster" ? grp_item.display_name.split("\\")[1] : grp_item.display_name
  group_display_name = (parent ? "#{parent.display_name}\\" : "") + grp_item.display_name
  grp_item.display_name = group_display_name

  stTypes = %W(VirtualMachine PhysicalMachine Storage)

  mach_item = grp_members.find{|m| stTypes.include?(m.class_name) }
  group_class = mach_item ? mach_item.class_name : grp_members.first.class_name

  if stTypes.include?(group_class) then
    # this group has PM or VM items - save it in the array to return
    machine_items = grp_members.select{|m| stTypes.include?(group_class)}
    
    group_name = grp_item.display_name
    new_grp_item = GroupStructure.new(grp_item, machine_items, "Folder", group_name, group_class, grp_item.report_enabled, grp_item.group_stats_enabled)
    map[grp_item.uuid] = new_grp_item

    if group_class == "PhysicalMachine" && grp_item.class_name!="Cluster"
      display_name = "VMs_"+grp_item.display_name
      display_name = display_name.gsub("VMs_PMs_","VMs_")
      vm_grp_item = TopologyItem.new("Group",grp_item.name+"-vms",display_name,grp_item.uuid+"-vms")
      vm_grp_members = get_members(machine_items, "Produces")
      new_vm_grp_item = GroupStructure.new(vm_grp_item, vm_grp_members, "Folder", vm_grp_item.display_name, "VirtualMachine", grp_item.report_enabled, grp_item.group_stats_enabled)
      map[vm_grp_item.uuid] = new_vm_grp_item
    end
  end

  for member in grp_members do
    if $GROUP_CLASSES.include?(member.class_name) then
      sub_groups = get_se_subgroups_map(member,grp_item)
      
      # Prefer the group with the longer name, which includes more
      # details about the Folder's place in the hierarchy.
      map.merge!(sub_groups){ |key, v1, v2|
        v1.group_name.length > v2.group_name.length ? v1 : v2
      }
    end
  end

  return map
end

def nested_groups_for(meta_group)
  arr = [ ]

  for uuid in $top_svc.get_instance_uuids(meta_group) do
    grp_item = make_item(uuid)
    arr = arr + get_se_subgroups(grp_item)
  end

  return arr.flatten
end

# # #

def persisted_group?(g_obj)
  $GROUP_CLASSES.include?(g_obj.class_name) || ["GROUP-PMsByCluster", "GROUP-VMsByCluster"].include?(g_obj.name)
end

def replace_groups(groups)
  groups = groups.flatten

  cl_group  = Classification.find_or_create_by_name("Group")
  cl_cluster = Classification.find_or_create_by_name("Cluster")
  cl_folder = Classification.find_or_create_by_name("Folder")
  cl_pool   = Classification.find_or_create_by_name("Pool")
  cl_vpool  = Classification.find_or_create_by_name("VPool")

  for g in groups do

    gobj = g.group_item

    if persisted_group?(gobj) then
      
      puts gobj.inspect if gobj.class_name=="StorageCluster"
      
      puts Time.now.to_s + "   Loading " + g.group_name + "(#{g.members.size.to_s} members)"

      uuid = gobj.uuid
      name = gobj.name
      display_name = gobj.display_name
      creation_class = "Group"

      e_obj = Entity.create({:name => name, :display_name => display_name, :uuid => uuid, :creation_class => creation_class})

      e_obj.classifications << cl_group
      e_obj.classifications << cl_cluster if gobj.class_name == "Cluster"
      e_obj.classifications << cl_folder if gobj.class_name == "Folder"
      e_obj.classifications << cl_pool   if gobj.class_name == "Pool"
      e_obj.classifications << cl_vpool  if gobj.class_name == "VPool"

      e_obj.classifications << Classification.find_or_create_by_name("Scope:#{g.scope_type}") unless g.scope_type.to_s.empty?

      e_obj.attrs.create({:name => "displayName",       :value => g.group_name})
      e_obj.attrs.create({:name => "SETypeName",        :value => (g.group_type=="VirtualDataCenter" ? "VirtualMachine" : g.group_type)})
      e_obj.attrs.create({:name => "reportEnabled",     :value => g.report_enabled})
      e_obj.attrs.create({:name => "groupStatsEnabled", :value => g.group_stats_enabled})

      assn_obj = e_obj.associations.create(:name => "AllGroupMembers")

      # Filter members to avoid duplicates, group subclasses are saved separately
      gMembers = []
      for member in g.members do
        gMembers << member unless ["Pool","VPool","Folder","Cluster","StorageCluster"].include?(member.class_name)
      end

      for member in gMembers do
        # create the member if it is not there
        unless ref_obj = Entity.find(:first, :conditions => "uuid = '#{member.uuid}'")
          ref_obj = Entity.create(:name => member.name, :display_name => member.display_name, :creation_class => member.class_name, :uuid => member.uuid)
          cl = Classification.find_or_create_by_name(member.class_name)
          ref_obj.classifications << cl
        end

        begin
          assn_obj.members << ref_obj
        rescue => e
          puts "     Item #{ref_obj.display_name.to_s} (#{ref_obj.uuid.to_s}) - #{e.message.to_s}"
        end
      end
    end

  end
end


def get_rightsizing_actions
  action_items = []
  act_item_uuids  = $top_svc.get_related_uuids("Market_ActionLog", "items")

  for ai_uuid in act_item_uuids do
    xml_for_ai = $api_svc.xml_for({ :svc => "ActionLogService", :method => "getByUuid", :args => [ai_uuid] })
    ai_attrs_from_xml = $top_svc.attrs_for(xml_for_ai,"ActionItem")
    ai_hash = $top_svc.shrink_attrs(ai_attrs_from_xml, ['savings','explanation','actionType','current','new','targetClassName', 'targetUuid'], {'targetClassName'=>'creationClassName', 'targetUuid'=>'uuid'})
    
    if ['RESIZE'].include?(ai_hash['actionType']) #'RIGHT_SIZE'      
      ai_attrs = {'obj_uuid'=>ai_hash['uuid'], 
        'details'=>{
          'explanation'=>ai_hash['explanation'],  
          'from'=>ai_hash['current'], 
          'to'=>ai_hash['new']
        }
      }
        
      begin
        ai_attrs['details']['savings']     = ai_hash['savings'].to_f.round.to_s
      rescue
        ai_attrs['details']['savings']     = "0.00"
      end
      
      action_items << ai_attrs
      # puts action_items
    end #IF RESIZE
  end #FOR ai_uuid
  
  return action_items
end

# # #

def get_license_data
  puts Time.now.to_s + " Begin Synching License Data"

  VcLicense.delete_all()

  for uuid in $top_svc.get_instance_uuids("LICENSE-COMMODITIES") do
    attrs = $top_svc.attrs_for($top_svc.xml_for(uuid,"capacity|used|product"))

    target, product = attrs['name'].to_s, attrs['product'].to_s
    capacity, used  = attrs['capacity'].to_s, attrs['used'].to_s
    db_attrs = {
      'target' => target,
      'product' => product,
      'capacity' => capacity,
      'used' => used,
    }

    puts " Key #{target}; Product: #{product}"

    VcLicense.create(db_attrs)
  end

  puts Time.now.to_s + " Done Synching of Licenses"
end

# # #

def update_se_info

  #
  #   Update individual SEs from the market
  #

  puts Time.now.to_s + " Synching SE information"

  mkt_uuid = $top_svc.get_related_uuids("MarketManager","MainMarket").first
  uuids    = $top_svc.get_related_uuids(mkt_uuid,"ServiceEntities")

  existing_uuids = Entity.find_by_sql("select distinct uuid from entities").collect{|e| e.uuid}

  new_uuids = uuids - existing_uuids  # set difference

  for uuid in new_uuids do
    se = make_item(uuid)
    Entity.create({:uuid => uuid, :creation_class => se.class_name, :name => se.name, :display_name => se.display_name})
    puts "Added #{se.display_name}"
  end


  updatable_uuids = uuids & existing_uuids  # set conjunction

  for uuid in updatable_uuids do
    se = make_item(uuid)
    se_entity = Entity.find_by_uuid(uuid)

    next if se_entity.nil?
    next if se.name == se_entity.name && se.display_name == se_entity.display_name

    se_entity.update_attributes({:uuid => uuid, :creation_class => se.class_name, :name => se.name, :display_name => se.display_name})
    puts "Updated #{se.display_name}"
  end

  delete_candidates = existing_uuids - uuids  # set difference

  for uuid in delete_candidates do
    se_entity = Entity.find_by_uuid(uuid)
    if $SE_CLASSES.include?(se_entity.creation_class)
      se_entity.destroy()
      puts "Removed obsolete #{se_entity.display_name}"
    end
  end


  puts Time.now.to_s + " Done synching SE information"
end

# # #

def get_wasted_files
  puts Time.now.to_s + " Synching wasted file information"

  EntityAttr.delete_all("name = 'wasted_file'")

  ds_items = Entity.find(:all, :conditions => "creation_class = 'Storage'")
  for ds in ds_items do
    xml = $top_svc.xml_for(ds.uuid,"WastedFile")

    begin
      doc   = REXML::Document.new(xml.to_s)
      root  = doc.root
      tag  = root.get_elements("./TopologyElement").first
      if !tag.nil?
        wasted_file_tags = tag.get_elements("./WastedFile")
  
        wasted_files = []
        for ftag in wasted_file_tags do
          attrs = ftag.attributes
          mtime = Time.at(attrs['lastModified'].to_i/1000).strftime("%Y-%m-%d")
          wasted_files << mtime + attrs['size'].rjust(14) + "  " + attrs['fileName']
        end
        wasted_files = wasted_files.sort{|a,b| a[25,200].to_s <=> b[25,200].to_s}
  
        for fname in wasted_files do
          ds.attrs.create({:name => 'wasted_file', :value => fname})
        end
      end
    rescue => e
      puts e.message
      puts e.backtrace.join("\n")
      doc = root = nil
      puts xml.to_s
    end
  end

  puts Time.now.to_s + " Done synching wasted file information"
end

# # #

def get_rightsizing_info
  puts Time.now.to_s + " Begin right-sizing info refresh"

  # clear out old right-sizing info
  EntityAttr.delete_all("name = 'RightsizingInfo'")

  # #

  rs_actions = get_rightsizing_actions()
  rs_uuids = rs_actions.collect{|a| a['obj_uuid']}.compact.sort.uniq

  for uuid in rs_uuids do
    se_entity = Entity.find_by_uuid(uuid)
    if se_entity then
      ais_for_obj = rs_actions.find_all{|a| a['obj_uuid'] == se_entity.uuid}
      for ai in ais_for_obj do
        se_entity.attrs.create({:name => "RightsizingInfo",
                                :multi_values => ai['details'].inspect.gsub('=>',':')
                                }
                              )
      end
    end
  end

  puts Time.now.to_s + " Done synching right-sizing info"
end


def refresh_groups
  puts Time.now.to_s + " Begin SE group refresh"

  # clear the database of obsolete group items
  $GROUP_CLASSES.each do |group_cls|
    Entity.destroy_all("creation_class = '#{group_cls}'")
  end
  Entity.destroy_all("creation_class = '' or creation_class is null")
  puts Time.now.to_s + " Cleared old group information"


  pm_group_scopes = %W(DataCenter Cluster)
  st_group_scopes = %W(DataCenter Cluster StorageCluster)
  vm_group_scopes = %W(Cluster PhysicalMachine Storage Network VirtualDataCenter)
  persisted_meta_groups_gen_members = %W(VMsByCluster PMsByCluster)

  for scope in pm_group_scopes do
    groups = groups_for("PhysicalMachineBy#{scope}",scope)
    replace_groups(groups)
  end

  for scope in st_group_scopes do
    groups = groups_for("StorageBy#{scope}",scope)
    replace_groups(groups)
  end

  groups = groups_for("StorageTiers","Storage")
  replace_groups(groups)

  for scope in vm_group_scopes do
    groups = groups_for("VirtualMachineBy#{scope}",scope)
    replace_groups(groups)
  end

  groups = groups_for("MyGroups","MyGroup")
  replace_groups(groups)

  groups = nested_groups_for("VCImportedGroups")
  replace_groups(groups)
  
  groups = nested_groups_for("VC-ANN")
  replace_groups(groups)
  
  for grp in persisted_meta_groups_gen_members do
    groups = meta_group_gen_members_for(grp)
    #puts groups.inspect
    replace_groups(groups)
  end

  # print_groups(groups)

  puts Time.now.to_s + " Groups refreshed"
end

# # #
# # #
# # #   M A I N
# # #
# # #

puts Time.now.to_s + " Begin platform info refresh"

  begin
    get_license_data()
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

  begin
    update_se_info()  # do this before others that attach info to SE entities
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

  begin
    get_rightsizing_info()  # attaches info to SE entities
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

  begin
    get_wasted_files()      # attaches info to SE entities
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

  begin
    refresh_groups()        # loads groups are relates them to SE members
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

puts Time.now.to_s + " Finished refresh"
  