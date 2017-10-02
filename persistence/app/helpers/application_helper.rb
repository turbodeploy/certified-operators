
  require 'rubygems'
#  require 'redcloth'
  require 'date'

# Add conversion method to get the date part of a Time (realized datetime from DB)

class Time 
    def to_date
        return Date.new(year,mon,day)
    end

    def to_date_image
        self.strftime("%Y-%m-%d")
    end

    def to_time_image
        self.strftime("%I:%M %p")
    end

    def to_datetime_image
        self.strftime("%Y-%m-%d %I:%M %p")
    end
end 

class Date 
    def to_date
        return self
    end

    def to_date_image
        self.to_s
    end

    def week_image
      jan1 = Date.new(year,1,1)
      day_no = jd() - jan1.jd()
      year.to_s + "-W" + (day_no/7+1).to_s    
    end
  
    def quarter_image
      year.to_s + "-Q" + (mon/4+1).to_s
    end
end 


# Methods added to this helper will be available to all templates in the application.

module ApplicationHelper

    def url_list(obj)
        return url_for( :controller => obj.ctrlr_name, :action => 'list' )
    end

    def url_show(obj)
        return url_for( :controller => obj.ctrlr_name, :action => 'show', :id => obj.id )
    end

    def url_show_properties(obj)
        return url_for( :controller => obj.ctrlr_name, :action => 'show_properties', :id => obj.id )
    end

    def url_show_as_doc(obj)
        return url_for( :controller => obj.ctrlr_name, :action => 'show_as_doc', :id => obj.id )
    end

    def url_edit(obj)
        return url_for( :controller => obj.ctrlr_name, :action => 'edit', :id => obj.id )
    end

    def url_destroy(obj)
        return url_for( :controller => obj.ctrlr_name, :action => 'destroy', :id => obj.id )
    end

    def url_public_home(user=session['user'])
        return url_for( :controller => 'public_pages', :action => 'main' )
    end

    def url_user_home(user=session['user'])
        return url_index
    end

    def url_index
        return url_for( :controller => 'index', :action => 'index' )
    end

    def to_html_attrs(attrs)
      text = ""
      attrs.each{|k,v| text = text + ' ' + k.to_s + '="' + v.to_s + '"'} 
      return text
    end

    def js_button_to(name,params={},opts={})
        url = url_for params
        text = '<input class="js_button" type="submit" value="' + name + '" onClick="location=' + "'" + url + "'" + '; return false;" ' + to_html_attrs(opts) + ' >'
        return text
    end

    def js_button_with_confirm_to(name,params={},opts={})
        url = url_for params
        text = '<input class="js_button" type="submit" value="' + name + '" onClick="if (confirmSubmit()) {location=' + "'" + url + "'" + '; return false;}" ' + to_html_attrs(opts) + ' >'
        return text
    end

    def submit_with_confirm_tag(verb,opts={})
        opts = { :onclick => "return confirmSubmit(#{opts[:confirm] ? ("'"+opts[:confirm].to_s+"'") : ""})" }.merge(opts)
        opts[:confirm] = nil
        return submit_tag(verb, opts)
    end

    def context_links obj
        rstr = ''
        c_obj = obj.container_obj
        if c_obj then
            rstr = rstr + '<p class="context"> Context: '
        
            hier = Array.new()
            while c_obj do 
                hier << c_obj
                c_obj = c_obj.container_obj
            end
            sep_needed = false
            hier.reverse_each { | hobj | 
                rstr = rstr + ' > ' if sep_needed
                rstr = rstr + link_to(hobj.iname, url_show(hobj), { :title => hobj.display_name } )
                sep_needed = true
            }
            rstr = rstr + ' </p> '
        end
        return rstr
    end


    def rc_format(text)
       while m = /\{image:([^}]+)\}/.match(text) do
          fname = m[1].strip
          if image_exist?(fname) then 
            linktext = image_tag("user_images/"+fname)
          else
            linktext = link_to(fname, { :controller => 'fast_ops', :action => 'images', :upload => fname}, :style => "color:red;")
          end

          text = m.pre_match + linktext + m.post_match
       end

       while m = /![><]?(\w[^.]+\.\w\w\w)!/.match(text) do
          fname = m[1].strip
          sfname = fname.gsub(" ","_").downcase
          if image_exist?(sfname) then
            img_path = image_path("user_images/"+sfname)
            text = m.pre_match + m[0].gsub(fname,img_path) + m.post_match
          else
            linktext = link_to(fname, { :controller => 'fast_ops', :action => 'images', :upload => sfname}, :style => "color:red;")
            text = m.pre_match + linktext + m.post_match
          end
       end

       while m = /\{doc:([^}]+)\}/.match(text) do
          fname = m[1].strip
          if doc_exist?(fname) then 
            linktext = link_to fname, image_path(fname).gsub("images","user_docs")
          else
            linktext = link_to(fname, { :controller => 'fast_ops', :action => 'docs', :upload => fname}, :style => "color:red;")
          end

          text = m.pre_match + linktext + m.post_match
       end

       if defined?(RedCloth)
          textilized = RedCloth.new(text)
          text = textilized.to_html
          text = auto_link_email_addresses(text)
       else
          text = text.gsub("\r\n","\n")
          text = text.gsub(/\n\s*\n/,"\n<br/>\n")
          text = auto_link(text)
       end

       text = text.gsub("<br></li>","</li>")

       return text
    end

    def strip_para(text)
       text = text[3..-1] if text[0..2] == "<p>"
       text = text[0..-5] if text[-4..-1] == "</p>"
       return text 
    end

    def doc_exist?(doc_name)
        return false if doc_name.nil?
        return File.exist?("#{RAILS_ROOT}/public/user_docs/#{doc_name}")
    end

    def image_exist?(img_name)
        return false if img_name.nil?
        return File.exist?("#{RAILS_ROOT}/public/images/user_images/#{img_name}")
    end

    def image_tag_or_crlink(img_name,alt_text="Image",width=nil)
        return "no-image-name" if img_name.nil?
        if File.exist?("#{RAILS_ROOT}/public/images/user_images/#{img_name}") then
            return width ? image_tag("user_images/"+img_name, :border => :none, :alt => alt_text, :width => width.to_s) : image_tag("user_images/"+img_name, :alt => alt_text) 
        else
            return link_to(alt_text, { :controller => 'fast_ops', :action => 'images', :upload => img_name, :id => 1, :style => 'compact' }, :style => "color:red;")
        end
    end

    def std_image_size(size)
        case size
            when :tiny
                16
            when :icon
                32
            when :small
                64
            when :medium
                128
            when :large
                256
            when :xlarge
                512
        end
    end

   # ---

   def child_nodes_for(node,opts,level)
     return "" if level > (opts[:max_depth] || 8)

     @visited = [] if @visited.nil?
     return "" if @visited.include?(node.class.name+node.id.to_s)
     @visited << node.class.name+node.id.to_s
     
     rstr = ""
     for child in node.get_children
        rstr = rstr + tree_for(child, opts, level+1)
     end

     return rstr
   end

   def expand_ctrl(title,nodes)
      return "&nbsp;&bull;" unless nodes.size > 0 
      rstr = <<-EOS
          <span style="cursor:pointer; width:15px; font-size:inherit; padding-left:3px; padding-right:2px;"
               onclick="Element.toggle('t_#{title}-#{@ord}'); 
               this.innerHTML = $('t_#{title}-#{@ord}').style.display == 'none' ? '+': '&#x2011;';"
               >+</span>
      EOS
      return rstr.strip
   end

   def labeled_level_for(nodes_hash,opts,level)
     title = nodes_hash[:title] || "Sub Items"
     title_link = nodes_hash[:title_link] 
     nodes = nodes_hash[:items] || []

     rstr = ""

     if nodes.size > 0 then
     
       rstr = rstr + <<-EOS1
         <div class="tree_lev1">
           #{expand_ctrl(title,nodes)}&nbsp;#{title + (title_link ? (" "+link_to("("+nodes.size.to_s+")", title_link)) : "")}
         </div>
       EOS1

       rstr = rstr + <<-EOS2
         <div class="tree_lev2" id="t_#{title}-#{@ord}" style="display:none;">
       EOS2
  
       for node in nodes
          rstr = rstr + tree_for(node, opts, level)
       end
  
       rstr = rstr + "\n       </div>"
     end 

     return rstr
   end

   def tree_for(node, opts={:action => "show"}, level=0)
     @ord = (@ord||0) + 1

     return labeled_level_for(node, opts, level) if node.instance_of?(Hash)
     
     return <<-EOS
       <div class="tree_lev1">
         <span style="cursor:pointer; width:15px; font-size:inherit; padding-left:3px; padding-right:2px;"
               onclick="Element.toggle('t_#{node.id}-#{@ord}'); 
               this.innerHTML = $('t_#{node.id}-#{@ord}').style.display == 'none' ? '+': '&#x2011;';"
               >+</span>&nbsp;#{link_to(node.iname, {:id => node,:controller => node.ctrlr_name}.merge(opts))}
       </div>
       <div class="tree_lev2" id="t_#{node.id}-#{@ord}" style="display:none;">
         #{child_nodes_for(node, opts, level)}
       </div>
       EOS
   end

   #
   # e.g. of items that support index view components 
   #
   # show_tree_for( { :obj_class => "State",
   #                  :title => "State".pluralize,
   #                  :title_link => url_for(:controller => 'states', :action => 'list'),
   #   optional -->   :items => State.items_for_index() } )
   #

   def show_tree_for(params, opts={:action => "show"}, level=0)
      params[:items] = params[:obj_class].constantize.items_for_index() if params[:items].nil?

      @visited = []
      return tree_for(params,opts,level)
   end

   def instance_count_for(params)
     rstr = <<-EOS1
       <div class="tree_lev1">
         &bull;&nbsp;#{params[:title]} (#{link_to(params[:obj_class].constantize.count(), params[:title_link])})
       </div>
     EOS1
     
     return rstr 
   end

   # ---

   def show_hide(label, div_id)
     return <<-EOS
       <span class="show_hide show_hide_#{div_id}" onClick="Element.toggle('#{div_id}')">#{label}</span>
       EOS
   end 

   def x_clear_for(ele_id, label="Dismiss")
     return <<-EOS
       <div class="x_dismiss" style="margin-right:10px;" onclick="$('#{ele_id}').update();">#{label}</div>
       EOS
   end

   def x_hide_for(ele_id, label="Dismiss")
     return <<-EOS
       <div class="x_dismiss" style="margin:10px;" onclick="Effect.Shrink('#{ele_id}');">#{label}</div>
       EOS
   end

   def edit_icon_for(obj)
     return link_to(image_tag("pencil.jpg", :height => "12", :border=>'none'), url_edit(obj), :style=>"background:inherit;", :title => 'Edit')
   end
  
  # ---

  def in_place_form_for(obj, properties=nil)
    return "" unless obj
    @obj, @properties = obj, (properties ? properties : obj.class.send("value_attrs"))

    return <<-EOS
      <div class="w20_in_place_form" id="w20_#{@obj.ctrlr_name.singularize}_div">
        #{render :partial => 'fast_ops/w20_show_object'}
      </div>
      EOS
  end

  def process_phone_param(obj,attr)
    params[obj][attr] = FtUtils.normalize_phone_no(params[obj][attr]) if params[obj][attr]
  end

    
  # --- div helpers ---  
    
  def div_for_topic(topic,opts={},&block)
    tidbit = topic.gsub(" ","").underscore
    style  = opts[:style]  ? (' style="'+opts[:style]+'"')  : ''
    tstyle = opts[:tstyle] ? (' style="'+opts[:tstyle]+'"') : ''
    cstyle = opts[:cstyle] ? (' style="'+opts[:cstyle]+'"') : ''
    str_before = <<-EOS1
      <div class="topic #{tidbit}_topic"#{style}>
        <div class="topic_heading #{tidbit}_heading"#{tstyle}>#{topic.to_s}</div>
        <div class="topic_content #{tidbit}_content" id="#{tidbit}_content"#{cstyle}>
      EOS1
    str_after = <<-EOS2
        </div>
      </div>
      EOS2
    concat str_before
    yield
    concat str_after
    nil
  end

  # ---

  def value_for(obj,attr,def_val="&nbsp;")
    return obj ? obj.send(attr).to_s : def_val
  end

  def value_or_default(val,def_val="&nbsp;")
    return val.to_s.empty? ? def_val : val
  end

  def money_value_for(obj,attr,def_val="&nbsp;")
    return obj ? number_to_currency(obj.send(attr).to_f) : def_val
  end

  def date_value_for(obj,attr,def_val="&nbsp;")
    return (obj && ! obj.send(attr).to_s.empty?) ? obj.send(attr).to_date_image : def_val
  end

  def time_value_for(obj,attr,def_val="&nbsp;")
    return (obj && ! obj.send(attr).to_s.empty?) ? obj.send(attr).to_time_image : def_val
  end

  def datetime_value_for(obj,attr,def_val="&nbsp;")
    return (obj && ! obj.send(attr).to_s.empty?) ? obj.send(attr).to_datetime_image : def_val
  end

  def iname_for(obj,rel_item,def_val="&nbsp;")
    return (obj && obj.send(rel_item)) ? obj.send(rel_item).iname : def_val
  end

  def number_value_for(obj,attr,def_val="&nbsp;")
    return obj ? number_with_delimiter(obj.send(attr).to_i) : def_val
  end

  def yesno_value_for(obj,attr,def_val="&nbsp;")
    return obj ? (obj.send(attr) ? "Yes" : "No") : def_val
  end

#BEGIN-UID.usermethods

  ENTITIES = { "&" => "&amp;", "<" => "&lt;", ">" => "&gt;", '"' => "&quot;"}
  ENTITY_CHARS = ["&", "<", ">", '"']

  def xml_encode(str)
    str = str.to_s

    for e in ENTITY_CHARS
      str = str.gsub(e,ENTITIES[e])
    end

    return str
  end

  # ---

  def delete_icon_for(obj)
    return link_to(image_tag("red_minus.jpg", :border => "none"), url_destroy(obj))
  end

  # ---

  def add_icon()
    return image_tag("green_plus.jpg", :border => "none")
  end

  # ---

class ::Time
    def to_db_time_str
        self.strftime("%Y-%m-%d %H:%M:%S")
    end
end


#END-UID.usermethods

end
