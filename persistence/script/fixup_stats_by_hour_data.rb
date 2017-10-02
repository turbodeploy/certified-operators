
#
# This routine fixes holes in hourly summary data due to system down-time
#
# Run like this:
#
#   # cd .../persistence
#   # script/console
#   >> source "script/fixup_stats_by_hour_data.rb"
#


#def klasses()
#  recs = SnapshotsStatsByHour.find_by_sql("select distinct class_name from snapshots_stats_by_hour")
#  items = recs.collect{|r| r.class_name}
#  return items
#end
#
#def items_for(col,klass)
#  recs = SnapshotsStatsByHour.find_by_sql("select distinct #{col} from snapshots_stats_by_hour where class_name = '#{klass}'")
#  items = recs.collect{|r| r.send(col)}
#  return items
#end
#
#def add_items_for(yyyy,mm,dy,hr)
#  for k in @class_names do
#    for pn in @prop_names[k] do
#      for uu in @uuids[k] do
#        time = Time.parse("#{yyyy}-#{mm}-#{dy} #{hr}:15:00")
#        t = SnapshotsStatsByHour.find(:first, :conditions => "class_name = '#{k}' and uuid = '#{uu}' and property_name = '#{pn}'")
#
#        if t then
#          t = t.clone
#          t.snapshot_time = time.to_i*1000
#          t.year_number = yyyy
#          t.month_number = mm
#          t.day_number = dy
#          t.hour_number = hr
#          t.short_day_name = time.strftime("%a")
#
#          t.save
#        end
#      end
#    end
#  end
#end


#@class_names = klasses()
#@prop_names  = {}
#@uuids = {}
#
#for k in @class_names do
#  @prop_names[k] = items_for('property_name',k)
#end
#
#for k in @class_names do
#  @uuids[k] = items_for('uuid',k)
#end
#
#
#first_day = Date.today-3
#last_day = Date.today-1
#yyyy = last_day.year.to_s
#mm = last_day.month.to_s.rjust(2,"0")
#
#
##TODO -- also fixup counts table
#
#
#for dy in (first_day.day.to_s.rjust(2,"0"))..(last_day.day.to_s.rjust(2,"0")) do
#  arr = SnapshotsStatsByHour.find :all, :conditions => "month_number = '#{mm}' and day_number = '#{dy}'"
#  hrs = arr.collect{|r| r.hour_number}.uniq.sort
#
#  for hr in "00".."23" do
#    unless hrs.include? hr
#      puts "Synthesizing data for day #{dy} and hour #{hr}"
#
#      add_items_for yyyy,mm,dy,hr
#    end
#  end
#end
#

puts "This script is obsolete"

