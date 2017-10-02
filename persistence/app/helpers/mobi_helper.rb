module MobiHelper

#BEGIN-UID.usermethods

  def prop_dname(name)
    case
    when name[/Q\d+VCPU/]
      name.gsub(/Q(\d+)VCPU/,'CPU Rdy \1')
    when name["Throughput"]
      name.gsub('Throughput','')
    when name == "VCPU"
      "vCPU"
    when name == "VMem"
      "vMem"
    when name == "StorageAmount"
      "Storage"
    when name == "StorageAccess"
      "IOPS"
    when name == "StorageLatency"
      "Latency"
    else
      name
    end
  end


#END-UID.usermethods

end
