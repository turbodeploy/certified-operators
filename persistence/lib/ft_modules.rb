
require 'erb'

module FtUtils

  def quoted_name(name)
    db_adapter = ActiveRecord::Base.configurations[RAILS_ENV]['adapter'].to_s
    if db_adapter["mysql"] then
      return "`#{name}`"
    else
      return '"' + name + '"'
    end
  end

  # ---

  def normalize_date_str(date_str)
    case
      when date_str.nil?
        return ""
      when date_str =~ /^\d\d\d\d-\d+-\d+$/
        return date_str
      when date_str =~ /^\d+\/\d+\/\d+$/
        mm, dd, yy = date_str.split("/")
        mm, dd, yy = mm.to_i, dd.to_i, yy.to_i
        yy = (yy >= 40 ? yy+1900 : yy+2000) if yy < 1000
        return "#{yy.to_s}-#{mm.to_s.rjust(2,'0')}-#{dd.to_s.rjust(2,'0')}"
      else
        return date_str
    end
  end

  def normalize_phone_no(pn)
    pn = pn.gsub(/^1-8/,"8")
    if m = /\D*(\d\d\d)\D*(\d\d\d)\D*(\d\d\d\d)\s*(.*)?/.match(pn.to_s) then
      pn = "#{m[1]}-#{m[2]}-#{m[3]}" + (m.size > 4 ? " #{m[4]}" : "") # ext 123
    end
    return pn
  end

  # ---

  def init_eval(expr)
    begin
      s = eval(expr)
      return s
    rescue
      return expr
    end
  end

  def adjust_sorting_and_filtering_params(klass,params)

    time_fields = klass.attr_type_info.select{|k,v| v[:datatype] == "time"}.collect{|kv| kv[0]}
    unless time_fields.empty?
      params[:search_str]  = Time.parse(params[:search_str].to_s).strftime("%H:%M:%S") if time_fields.include?(params[:search_field].to_s)
      params[:search_str2] = Time.parse(params[:search_str2].to_s).strftime("%H:%M:%S") if time_fields.include?(params[:search_field2].to_s)
      params[:search_str3] = Time.parse(params[:search_str3].to_s).strftime("%H:%M:%S") if time_fields.include?(params[:search_field3].to_s)
    end

    args = [ 
      [params[:search_field].to_s, params[:search_str].to_s],
      [params[:search_field2].to_s, params[:search_str2].to_s],
      [params[:search_field3].to_s, params[:search_str3].to_s]
    ]    
    
    args.delete_if{|arg| arg[0].to_s.empty?}
            
    where_parts = params[:where].to_s.empty? ? [] : [ params[:where] ]

    for arg in args do
      field_name = arg[0]
      op_n_str = arg[1].gsub('*','%').strip
      
      has_not = (op_n_str =~ /^not\s+/)
      op_n_str = op_n_str.gsub(/^not\s+/,'') if has_not

      date_val = op_n_str[/\d+\/\d+\/\d+/]
      op_n_str = op_n_str.gsub(date_val,normalize_date_str(date_val)) if date_val
      
      case
        when op_n_str.empty?
          op_n_str = "= ''"
        when op_n_str.include?("%")
          op_n_str = "like '#{op_n_str}'"       unless has_not
          op_n_str = "not like '#{op_n_str}'"   if has_not
        when op_n_str.downcase.include?("null")
          op_n_str = "is #{op_n_str}"   # covers null and not null
        when op_n_str.include?("|")
          op_n_str = "in ('#{op_n_str.split("|").join("','")}')"        unless has_not
          op_n_str = "not in ('#{op_n_str.split("|").join("','")}')"    if has_not
        when ["<",">","<=",">="].include?(op_n_str.slice(0,2).strip)
          op_n_str = op_n_str.gsub(/(..)\s*(.*)/,"\\1 '\\2'")   unless op_n_str.include?("(")
          op_n_str = op_n_str.gsub(/(..)\s*(.*)/,"\\1 \\2")     if op_n_str.include?("(")
          op_n_str << " ''" if op_n_str.size <= 2
        else
          op_n_str = "= '#{op_n_str}'"    unless has_not
          op_n_str = "<> '#{op_n_str}'"   if has_not
          op_n_str = op_n_str.gsub("'true'","TRUE").gsub("'false'","FALSE")
      end
      
      where_parts << "#{field_name} #{op_n_str}"
    end
       
    params[:where] = where_parts.join(" AND ")
    params[:where] = nil if params[:where].to_s.empty?

    unless params[:sort_field].to_s.empty? 
      params[:order_by] = quoted_name(params[:sort_field].to_s) + " #{params[:direction]}"
      unless params[:sort_field2].to_s.empty? 
        params[:order_by] = params[:order_by] + ", " + quoted_name(params[:sort_field2].to_s) + " #{params[:direction2]}"
        unless params[:sort_field3].to_s.empty? 
          params[:order_by] = params[:order_by] + ", " + quoted_name(params[:sort_field3].to_s) + " #{params[:direction3]}"
        end
      end
    end

  end

  def merge_duplicates(codetable_name)
    map_name_to_entry = {}
    entries = codetable_name.find(:all, :order => 'id desc')
    for entry in entries do 
      existing_entry = map_name_to_entry[entry.iname]
      if existing_entry then
        existing_entry.merge(entry)
        entry.destroy
      else
        map_name_to_entry[entry.iname] = entry
      end
    end
  end


  class FtErbExpander
    def initialize(var_hash)
      @get = var_hash.clone
      @get.default = "NO SUCH VALUE"

      @get[:today] = Date.today
      @get[:date] = Date.today.to_date_image
      @get[:long_date] = Date.today.strftime("%B %d, %Y")
    end

    def expand(str)
      template = ERB.new(str)
      return template.result(binding)
    end
  end

  # ---

  def erb_expand(template_str,var_hash={})
    expander = FtErbExpander.new(var_hash)
    return expander.expand(template_str)
  end


#BEGIN-UID.usermethods

  def file_for_uuid(uuid)
    return RAILS_ROOT+"/doc_cache/#{uuid}.xml"
  end

  def entity_cache_get(uuid)
    fn = file_for_uuid(uuid)
    return File.exists?(fn) ? File.read(fn) : nil
  end

  def entity_cache_put_xml(uuid,xml)
    unless uuid.to_s.empty?
      Dir.mkdir(RAILS_ROOT+"/doc_cache") unless File.exists?(RAILS_ROOT+"/doc_cache")
      File.open(file_for_uuid(uuid),"w") { |f| f.write(xml) }
    end
  end

  def entity_cache_put(entity)
    entity_cache_put_xml(entity.uuid,entity.to_xml(:skip_instruct => true))
  end

  def entity_cache_remove(entity)
    File.delete(file_for_uuid(entity.uuid)) if File.exists?(file_for_uuid(entity.uuid))
  end

  def db_adapter_name
    return ActiveRecord::Base.configurations[RAILS_ENV]['adapter'].to_s
  end

  def apply_limit(sql)
    @page_no    = (@page_no || 1)
    @page_size  = (@page_size || 25)
    @offset     = (@page_no-1)*@page_size

    adapter = db_adapter_name
    case adapter
    when 'mysql'
      sql << " LIMIT #{@page_size.to_s} OFFSET #{@offset.to_s}"
    when 'sqlserver'
      sql = sql.gsub(/^\s*SELECT/,"SELECT TOP #{@page_size.to_s}")
    else
      puts "Error in function apply_limit: unknown db adapter: " + adapter
      sql << " LIMIT #{@page_size.to_s}"
    end

    return sql
  end

  module_function :file_for_uuid
  module_function :entity_cache_get
  module_function :entity_cache_put
  module_function :entity_cache_put_xml
  module_function :entity_cache_remove
  module_function :db_adapter_name
  module_function :apply_limit

#END-UID.usermethods

   module_function :quoted_name
   module_function :normalize_phone_no
   module_function :init_eval
   module_function :adjust_sorting_and_filtering_params
   module_function :merge_duplicates
   module_function :erb_expand
end
