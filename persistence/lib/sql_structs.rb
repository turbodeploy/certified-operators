
require 'yaml'
require 'active_record'


REAL_OP = { 'equal' => '=',  'not equal' => '<>',
            'greater than' => '>',  'greater than or equal' => '>=',
            'less than' => '<',  'less than or equal' => '<=',
            'in' => 'in', 'not in' => 'not in',
            'between' => 'between',
            'like' => 'like',
            'not like' => 'not like'
            }

REAL_OP_INV = REAL_OP.invert


STCondition = Struct.new(:field, :op, :operands) do
  def val_expr(s)
    return s if s =~ /^(\d|\.)+$/
    return s if s[/[a-zA-Z_]+\(/]
    return s if s[/^select/]
    return "'"+s+"'" unless s[0,1] == "'"
  end

  def opnds
    vals = [self.operands].flatten.collect{|v| val_expr(v)}
    case self.op
      when 'between'
        return vals.join(" and ")
      when 'in','not in'
        return "(" + vals.join(",") + ")"
      when 'is null', 'is not null'
        return REAL_OP[op] || op
      else
        return vals[0].nil? ? "''" : vals[0]
    end
  end

  def to_sql
    return "#{self.field.name} #{self.op} " + opnds()
  end
end

STField = Struct.new(:name, :is_selected, :col_order, :conditions, :sorting, :sort_order) do
  def new_condition(*args)
    self.conditions = [] unless self.conditions
    c = STCondition.new(*([self]+args))
    self.conditions << c
    return c
  end
end

STQuery = Struct.new(:table, :title, :category, :short_desc, :description, :limit, :fields) do
  def new_field(*args)
    self.fields = [] unless self.fields
    f = STField.new(*args)
    self.fields << f
    return f
  end

  def get_field(fname)
    return self.fields.to_a.find{|f| f.name == fname}
  end

  def to_sql(aliases={})
    selected_fields = self.fields.to_a.select{|f| f.is_selected}.sort{|a,b| a.col_order.to_i <=> b.col_order.to_i}

    sql_str = "SELECT "
    sql_str << (selected_fields.empty? ? "*" : selected_fields.collect{|f| "#{aliases[f.name] || f.name}"}.join(","))
    sql_str << " FROM " + self.table

    where_terms = self.fields.to_a.collect{|f| f.conditions.to_a}.compact.flatten.collect{|c| c.to_sql}.join(" AND ")
    if where_terms.size > 0 then
      sql_str << " WHERE " + where_terms
    end

    order_terms = selected_fields.select{|f| f.sorting}.compact
    order_terms = order_terms.sort{|a,b| a.sort_order.to_i <=> b.sort_order.to_i}
    order_terms = order_terms.collect{|f| "#{f.name} #{f.sorting.upcase}"}.join(",")
    if order_terms.size > 0 then
      sql_str << " ORDER BY " + order_terms
    end

    if self.limit.to_i > 0 then
      adapter = ActiveRecord::Base.configurations[RAILS_ENV]['adapter'].to_s
      case adapter
      when 'mysql'
        sql_str << " LIMIT " + self.limit.to_s
      when 'sqlserver'
        sql_str = sql_str.gsub(/^SELECT/,"SELECT TOP #{self.limit.to_s}")
      else
        puts "Unknown DB Adapter: " + adapter + " ... appending LIMIT clause"
        sql_str << " LIMIT " + self.limit.to_s
      end
    end

    return sql_str
  end

  def to_xml
    xml_str = String.new

    xml = Builder::XmlMarkup.new(:indent => 2, :target => xml_str)
    xml.instruct!

    xml.user_report do
      
      xml.title self.title.to_s
      xml.table self.table.to_s
      xml.limit self.limit.to_s

      xml.fields do
        selected_fields = self.fields.to_a.select{|f| f.is_selected}.sort{|a,b| a.col_order.to_i <=> b.col_order.to_i}
        selected_fields.collect{|f| f.name}.each { |f|
          xml.field f
        }
      end

      xml.filter_by do
        fields.to_a.collect{|f| f.conditions.to_a}.flatten.compact.each do |c|
          xml.filter do
            xml.field c.field.name
            xml.op(REAL_OP_INV[c.op] || c.op)
            xml.value [c.operands].flatten.join(",")
          end
        end
      end

      xml.order_by do
        order_fields = fields.to_a.select{|f| f.sorting}.sort{|a,b| a.sort_order.to_i <=> b.sort_order.to_i}
        idx = 1
        order_fields.each do |f|
          xml.order do
            xml.field f.name
            xml.direction f.sorting
            xml.order idx
            idx += 1
          end
        end
      end

    end

    return xml_str
  end

end



require 'rexml/document'

def content_of(node)
  return node ? node.get_text.to_s : ""
end

def STQuery.new_from_xml(xml_desc)

  xml_doc = REXML::Document.new(xml_desc)
  root = xml_doc.root

  query  = STQuery.new

  query.table = content_of(root.elements["./table"])
  query.limit = content_of(root.elements["./limit"])

  xml_fields = root.get_elements("./fields/field")
    idx = 1
    xml_fields.each{ |f|
      query.new_field(content_of(f),true,idx)
      idx += 1
    }
  xml_fields = nil

  xml_filters = root.get_elements("./filter_by/filter")
    xml_filters.each{|filter|
      fname = content_of(filter.elements["./field"])
      f = query.get_field(fname)
      f = query.new_field(fname) unless f
      op = content_of(filter.elements["./op"])
      values = content_of(filter.elements["./value"])
      if values[/[a-zA-Z_]+\(/] then # function call in the mix?
        values = [ values ]
      else
        values = values.split(",")
      end
      f.new_condition((REAL_OP[op] || op),values)
    }
  xml_filters = nil

  xml_orders = root.get_elements("./order_by/order")
    i = 1
    xml_orders.each{|order|
      fname = content_of(order.elements["./field"])
      f = query.get_field(fname)
      f = query.new_field(fname) unless f
      f.sorting = content_of(order.elements["./direction"])
      idx = content_of(order.elements["./sort_order"])
      f.sort_order = idx.empty? ? i : idx.to_i
      i += 1
    }
  xml_orders = nil

  return query
end


# ------------------------ usage ----------------------


if ARGV and ARGV[0] == 'test'

  q  = STQuery.new('snapshots','Top 10 CPU  Users','Capacity','Top 10 CPU','Top 10 CPU',10)

  f = q.new_field('class_name',false,nil)
  f.new_condition('in', 'PhysicalMachine')

  f = q.new_field('instance_name',true,2)
  f.new_condition('like','dell%');
  f.new_condition('<>','dell2.corp.vmturbo.com')

  f = q.new_field('property_type',true,3)
  f.new_condition('=','CPU')
  f.sorting = 'asc'

  f = q.new_field('property_subtype',true,4)
  f.sorting = 'asc'

  f = q.new_field('property_value',true,5)

  yml = q.to_yaml
  q2 = YAML::load(yml)

  puts
  puts q2.to_sql
  puts

  xml = q2.to_xml
  #puts xml

  q3 = STQuery.new_from_xml(xml)

  puts
  puts q3.to_sql
  puts

  puts q2.get_field('property_type').inspect

end


