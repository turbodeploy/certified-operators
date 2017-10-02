class UserTablesController < ApplicationController

  before_filter :login_required

  USER_TABLES = [
    {
      :name => 'user_pm_stats_by_hour',
      :enum_source => 'pm_stats_by_hour',
      :ecode => 'pm',
      :purpose => 'Host Machine hourly utilization summary report',
      :description =>   "This type of report is intended for use in assessing current trends
                        in utilization spanning hours of operation.
                        It keeps a limited number of hours (72 or more) of historical data, capturing
                        rolled up resource measurement statistics for each measured property.
                        ",
      :fields => [
           { :name => "instance_name", :type => "string", :enum => true },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
           { :name => "hour_number", :type => "string" },
         ]

    },
    {
      :name => 'user_vm_stats_by_hour',
      :enum_source => 'vm_stats_by_hour',
      :ecode => 'vm',
      :purpose => 'Virtual Machine hourly utilization summary report',
      :description =>   "This type of report is intended for use in assessing current trends
                        in utilization spanning hours of operation.
                        It keeps a limited number of hours (72 or more) of historical data, capturing
                        rolled up resource measurement statistics for each measured property.
                        ",
      :fields => [
           { :name => "instance_name", :type => "string", :enum => true },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
           { :name => "hour_number", :type => "string" },
         ]
    },
    {
      :name => 'user_vm_stats_by_hour_per_group',
      :enum_source => 'vm_stats_by_hour',
      :ecode => 'vm',
      :purpose => 'Virtual Machine hourly utilization summary by group',
      :description =>   "Same as above, but information is provided by vm group.
                        ",
      :fields => [
           { :name => "group_name", :type => "string" },
           { :name => "instance_name", :type => "string" },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
           { :name => "hour_number", :type => "string" },
         ]
    },
    {
      :name => 'user_ds_stats_by_hour',
      :enum_source => 'ds_stats_by_hour',
      :ecode => 'ds',
      :purpose => 'Datastore hourly utilization summary report',
      :description =>   "This type of report is intended for use in assessing current trends
                        in utilization spanning hours of operation.
                        It keeps a limited number of hours (72 or more) of historical data, capturing
                        rolled up resource measurement statistics for each measured property.
                        ",
      :fields => [
           { :name => "instance_name", :type => "string", :enum => true },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
           { :name => "hour_number", :type => "string" },
         ]
    },
    {
      :name => 'user_pm_stats_by_day',
      :enum_source => 'pm_stats_by_day',
      :ecode => 'pm',
      :purpose => 'Host machine daily utilization summary report',
      :description =>   "This type of report is intended for use in assessing current trends
                        in utilization spanning days of operation.
                        It keeps a limited number of days (60 or more) of historical data, capturing
                        rolled up resource measurement statistics for each measured property.
                        ",
      :fields => [
           { :name => "instance_name", :type => "string", :enum => true },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
         ]
    },
    {
      :name => 'user_vm_stats_by_day',
      :enum_source => 'vm_stats_by_day',
      :ecode => 'vm',
      :purpose => 'Virtual Machine daily utilization summary report',
      :description =>   "This type of report is intended for use in assessing current trends
                        in utilization spanning days of operation.
                        It keeps a limited number of days (60 or more) of historical data, capturing
                        rolled up resource measurement statistics for each measured property.
                        ",
      :fields => [
           { :name => "instance_name", :type => "string", :enum => true },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
         ]
    },
    {
      :name => 'user_vm_stats_by_day_per_group',
      :enum_source => 'vm_stats_by_day',
      :ecode => 'vm',
      :purpose => 'Virtual Machine daily utilization summary by group',
      :description =>   "Same as above, but information is provided by vm group.
                        ",
      :fields => [
           { :name => "group_name", :type => "string" },
           { :name => "instance_name", :type => "string" },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
         ]
    },
    {
      :name => 'user_ds_stats_by_day',
      :enum_source => 'ds_stats_by_day',
      :ecode => 'ds',
      :purpose => 'Datastore daily utilization summary report',
      :description =>   "This type of report is intended for use in assessing current trends
                        in utilization spanning days of operation.
                        It keeps a limited number of days (60 or more) of historical data, capturing
                        rolled up resource measurement statistics for each measured property.
                        ",
      :fields => [
           { :name => "instance_name", :type => "string", :enum => true },
           { :name => "property_type", :type => "string", :enum => true },
           { :name => "property_subtype", :type => "string", :enum => true },
           { :name => "capacity", :type => "float" },
           { :name => "avg_value", :type => "float" },
           { :name => "min_value", :type => "float" },
           { :name => "max_value", :type => "float" },
           { :name => "recorded_on", :type => "date" },
         ]
    },
    {
      :name => 'user_vm_storage',
      :enum_source => 'vm_storage',
      :ecode => 'vm',
      :purpose => 'VM usage of Datastore storage amounts',
      :description =>   "Shows VMs, their datastores and the amount of storage used.",
      :fields => [
           { :name => "vm_name", :type => "string", :enum => true },
           { :name => "ds_name", :type => "string", :enum => true },
           { :name => "storage_amount", :type => "float" },
         ]
    },
  ]


  def self.params_for(name)
    return USER_TABLES.find{|ut| ut[:name] == name}
  end
  

  def index
    @user_tables = USER_TABLES
    render :type => :builder,
           :inline => <<-STR
                      xml.instruct!
                      xml.tag! 'user-tables' do
                        for t in @user_tables do
                          xml.tag! 'user-table', { :name => t[:name], :purpose => t[:purpose], :description => t[:description].gsub("\n","").gsub(/\s+/," ") } do
                            for f in t[:fields] do
                              xml.tag! 'column', f
                            end
                          end
                        end
                      end
                  STR
  end


  def get_values
    @table_name = params[:table_name]
    @field_name = params[:field_name]

    if @table_name.to_s.empty? || @field_name.to_s.empty? then
      render :text => "Must supply table_name and field_name params"
      return
    end

    sql = "SELECT DISTINCT #{@field_name} FROM #{@table_name} ORDER BY #{@field_name}"
    
    begin
      @rows = UserReport.find_by_sql(sql)
    rescue
      @rows = UserReport.find(:all, :conditions => "title is null")
    end

    render :type => :builder,
           :inline => " xml.instruct!
                        xml.values {
                          for row in @rows do
                            xml.value row.send(@field_name)
                          end
                        }
                      "
  end


end
