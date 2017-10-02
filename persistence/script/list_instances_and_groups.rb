
puts

puts
puts "PMs:"
for e in Entity.find_by_sql("select distinct display_name from entities where creation_class = 'PhysicalMachine' and display_name is not null order by display_name") do
  puts "    " + e.display_name
end

puts
puts "PM Groups:"
class_name = 'PhysicalMachine'
items = StandardReport.find_by_sql("select group_name from entity_groups where group_type = '#{class_name}'")
items = items.collect{|r| r.group_name}.select { |n| ! n.to_s.empty? }
for e in items do
  puts "    " + e.to_s
end

puts
puts "VMs:"
for e in Entity.find_by_sql("select distinct display_name from entities where creation_class = 'VirtualMachine' and display_name is not null order by display_name") do
  puts "    " + e.display_name
end

puts
puts "VM Groups:"
class_name = 'VirtualMachine'
items = StandardReport.find_by_sql("select group_name from entity_groups where group_type = '#{class_name}'")
items = items.collect{|r| r.group_name}.select { |n| ! n.to_s.empty? }
for e in items do
  puts "    " + e.to_s
end

puts

