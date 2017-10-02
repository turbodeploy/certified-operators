
#
# This routine creates test data
#
# Run like this:
#
#   # cd .../persistence
#   # script/console
#   >> source "script/synthesize_by_hour_data.rb"
#



require 'topology_svc'




CapacityOf = {
  'Ballooning'=>58717387.0,
  'CPU'=>35360.0,
  'Cooling'=>1000.0,
  'IOThroughput'=>512000.0,
  'Mem'=>8386560.0,
  'NetThroughput'=>256000.0,
  'Power'=>1000.0,
  'Q1VCPU'=>20000.0,
  'Q2VCPU'=>40000.0,
  'Q4VCPU'=>80000.0,
  'Q8VCPU'=>160000.0,
  'Q16VCPU'=>320000.0,
  'Space'=>1000.0,
  'StorageAccess'=>500.0,
  'StorageAmount'=>953341.0,
  'StorageLatency'=>1000.0,
  'Swapping'=>5000000.0,
  'VCPU'=>35360.0,
  'VMem'=>4194304.0,
}


entities = Entity.find(:all, :conditions => "creation_class in ('PhysicalMachine','VirtualMachine','Storage')")
pms = entities.select{|e| e.creation_class == 'PhysicalMachine'}
vms = entities.select{|e| e.creation_class == 'VirtualMachine'}
dss = entities.select{|e| e.creation_class == 'Storage'}


def add_ut_items_for(t,ecode,items,commodities)

  klass = "#{ecode}StatsByHour"
  klass = klass.constantize

  for item in items do
#puts "adding data for #{item.display_name} at time #{t.to_s}"
    for comm in commodities do
      avg = rand()
      klass.create(
          {
            :snapshot_time => (t.to_f*1000).to_i,
            :uuid => item.uuid,
            :property_type => comm,
            :property_subtype => 'utilization',
            :capacity => CapacityOf[comm] || 0.0,
            :avg_value => avg,
            :min_value => [0.0,avg-rand()/10.0].max,
            :max_value => [100.0,avg+rand()/10.0].min,
            :std_dev => rand()/30.0,
          }
        )
    end
  end

end


def add_p_and_p_items_for(t,ecode,items)

  klass = "#{ecode}StatsByHour"
  klass = klass.constantize

  for item in items do
#puts "adding data for #{item.display_name} at time #{t.to_s}"
    if ecode == 'Pm'
      for comm in ['Produces'] do
        avg = 12
        klass.create(
            {
              :snapshot_time => (t.to_f*1000).to_i,
              :uuid => item.uuid,
              :property_type => comm,
              :property_subtype => 'used',
              :capacity => CapacityOf[comm] || 0.0,
              :avg_value => avg,
              :min_value => [0.0,avg-rand()/10.0].max,
              :max_value => [100.0,avg+rand()/10.0].min,
              :std_dev => rand()/30.0,
            }
          )
      end
    end
    for comm in ['priceIndex'] do
      avg = 200*rand()
      klass.create(
          {
            :snapshot_time => (t.to_f*1000).to_i,
            :uuid => item.uuid,
            :property_type => comm,
            :property_subtype => 'used',
            :capacity => CapacityOf[comm] || 0.0,
            :avg_value => avg,
            :min_value => [0.0,avg-rand()/10.0].max,
            :max_value => [100.0,avg+rand()/10.0].min,
            :std_dev => rand()/30.0,
          }
        )
    end
  end

end


def comm_subtypes(comm)
  case comm
  when "StorageAmount"
    return ['used','unused','conf','swap','snapshot','log','disk']
  else
    return ['used']
  end
end


def add_us_items_for(t,ecode,items,commodities)

  klass = "#{ecode}StatsByHour"
  klass = klass.constantize

  for item in items do
#puts "adding data for #{item.display_name} at time #{t.to_s}"
    all_comms = commodities
    all_comms = commodities+[%W(Q1VCPU Q2VCPU Q4VCPU Q8VCPU Q16VCPU)[(item.uuid.hash%5).to_i]] if ecode == 'Vm'
    for comm in all_comms do
      for subt in comm_subtypes(comm) do
        cap = CapacityOf[comm] || 0.0
        avg = rand()
        klass.create(
            {
              :snapshot_time => (t.to_f*1000).to_i,
              :uuid => item.uuid,
              :property_type => comm,
              :property_subtype => subt,
              :capacity => cap,
              :avg_value => avg*cap,
              :min_value => avg*cap,
              :max_value => avg*cap,
              :std_dev => 0.0,
            }
          )
      end
    end
  end

end



pm_ut_commodities = %W(CPU Mem Ballooning Swapping IOThroughput NetThroughput Q1VCPU Q2VCPU Q4VCPU Q8VCPU Q16VCPU)
vm_ut_commodities = %W(VCPU VMem)
ds_ut_commodities = %W(StorageAmount StorageAccess StorageLatency)

vm_us_commodities = %W(StorageAmount StorageAccess StorageLatency)
ds_us_commodities = %W(StorageAmount)

n_days = 15

first_day = Date.today-n_days

for d in 0..n_days do
  puts "#{(first_day+d).to_s}"
  for h in 00..23 do
    t = Time.parse("#{(first_day+d).to_s} #{h}:15:00")
    add_ut_items_for(t,'Pm',pms,pm_ut_commodities)
    add_p_and_p_items_for(t,'Pm',pms)
    add_p_and_p_items_for(t,'Vm',vms)
    add_ut_items_for(t,'Vm',vms,vm_ut_commodities)
    add_us_items_for(t,'Vm',vms,vm_us_commodities)
    add_us_items_for(t,'Ds',dss,ds_us_commodities)
    add_ut_items_for(t,'Ds',dss,ds_ut_commodities)
  end
end
