#!/usr/bin/ruby
=begin
VMTurbo Network Configuration Script
=end

# Includes
# Needed for IP validation
require "resolv"

machine_config = Hash.new
$nic_name = `ls /sys/class/net/ | grep -v lo`.strip
$path_to_ifcfg = "/etc/sysconfig/network/ifcfg-"+$nic_name
$path_to_dns = "/etc/resolv.conf"
$path_to_route = "/etc/sysconfig/network/routes"

# Methods

# Get user input and remove newline char
def ask (input)
    puts "#{input}\n"
    answer = gets.chomp
    return answer.downcase
end

def choose_if_static_or_dhcp
    setting = ""
    while setting != "dhcp" and setting != "static"
        setting = ask("Do you want to use DHCP or set a static IP (dhcp/static) ::") { |q| q.echo = true }
        if setting != "dhcp" and setting != "static"
            puts "You have entered an invalid setting. Please choose 'static' or 'dhcp'\n"
        else
            # Exit the while loop once a valid entry has been made
            break
        end
    end
    return setting
end

#Set DHCP if the user doesn't want a static IP anymore
def write_dhcp_settings_to_config_file()
    # Write DHCP configuration to the ifcfg config file
    network_config = File.open($path_to_ifcfg, "w")
    network_config.puts 'STARTMODE="auto"'
    network_config.puts 'DEVICE="' + $nic_name + '"'
    network_config.puts "BOOTPROTO=dhcp"
    network_config.puts 'ONBOOT="yes"'
    network_config.close

    # Remove the static routes file (not required with DHCP)
    File.delete($path_to_route) if File.exist?($path_to_route)
end

def write_static_ip_to_config_file(ip_address, netmask, gateway)
    network_config = File.open($path_to_ifcfg, "w")
    network_config.puts "STARTMODE=\"auto\""
    network_config.puts "DEVICE=\"" + $nic_name + "\""
    network_config.puts "BOOTPROTO=static \n"
    network_config.puts "ONBOOT=\"yes\""
    network_config.puts "IPADDR=#{ip_address}\n"
    network_config.puts "NETMASK=#{netmask}\n"
    network_config.puts "GATEWAY=#{gateway}\n"
    network_config.close
end

def set_default_routing_table(gateway)
    route_config = File.open($path_to_route, "w")
    route_config.puts "default #{gateway} - -"
    route_config.close
end

def save_dns_servers_to_config_file(dns)
    dns_config = File.open($path_to_dns, "w")
    # 'dns' is always an array of strings
    dns.each do |i|
        dns_config.puts "nameserver #{i}\n"
    end
    dns_config.close
end

#IP Validation for IP address, Gateway address, and DNS Server IPs
def ip_validate(ip_to_validate, config_item)
    ip_address = ip_to_validate
    while (!(ip_address =~ Resolv::IPv4::Regex))
        puts "#{config_item} is empty or not valid.\n"
        ip_address = ask("Please re-enter the #{config_item} for this machine :: ") { |q| q.echo = true }
    end
    return ip_address
end

# get IP address, DNS Server IP, and Gateway
def get_ip_config_item(config_item)
	ip_address = ask("Please enter the " + config_item + " for this machine :: ") { |q| q.echo = true }

	# Confirm it's a valid IP address
	ip_address = ip_validate(ip_address, config_item)
	return ip_address
end

#def get_machine_ip
#    config_item = "IP Address"
#    ip_address = ask("Please enter the IP address for this machine ::")  { |q| q.echo = true }
#
#    #Check if ip_address is a valid IP Address. Prompt again if it's not
#    while (!(ip_address =~ Resolv::IPv4::Regex))
#        puts "#{config_item} is empty or not valid. \n"
#        ip_address = ask("Please re-enter the #{config_item} for this machine :: ") { |q| q.echo = true }
#    end
#    return ip_address
#end

def get_network_mask
    #Define netmask regex pattern
    netmask_pattern = /^(((128|192|224|240|248|252|254)\.0\.0\.0)|(255\.(0|128|192|224|240|248|252|254)\.0\.0)|(255\.255\.(0|128|192|224|240|248|252|254)\.0)|(255\.255\.255\.(0|128|192|224|240|248|252|254)))$/i
    netmask = ask("Please enter the network mask for this machine ::")  { |q| q.echo = true }

    #Check if netmask a valid subnet mask. Prompt again if it's not
    while !netmask.match(netmask_pattern)
        puts "Subnet Mask '" + netmask + "' is not a valid subnet mask."
        netmask = ask("Please re-enter the network mask for this machine ::")  { |q| q.echo = true }
    end
    return netmask
end

def get_dns_servers
        #Setting DNS array
        config_item = "DNS Server IP Address"
        dns_servers = Array.new
        dns_answer = "y"

        #Getting DNS entries
        while dns_answer != "n"
            dns_entry = ask("Please enter the " + config_item + " for this machine :: ") { |q| q.echo = true }

            #If Gateway IP is empty or not valid
            dns_entry = ip_validate(dns_entry, config_item)
            dns_servers.push dns_entry

            #Asking for another DNS server
            dns_answer = ask("Would you like to add another DNS Server? (y/n)") { |q| q.echo = true }
            while !(dns_answer == "n" or dns_answer == "y")
                dns_answer = ask("Invalid selection. Please enter y or n ::") { |q| q.echo = true }
            end
        end
        return dns_servers
end

def collect_static_ip_settings()
    ip_address = get_ip_config_item("IP Address")
    netmask = get_network_mask()
    gateway = get_ip_config_item("Gateway address")
    dns = get_dns_servers()
    return ip_address, netmask, gateway, dns
end

def confirm_settings_with_user(ip_address, netmask, gateway, dns)
    puts "-----"
    puts "These are the settings that will be committed."
    puts "The IP Address is " + ip_address
    puts "The Netmask is " + netmask
    puts "The Gateway is " + gateway

    # If user entered multiple DNS servers, iterate through the array. Otherwise print the only one
    dns.each do |dns_server|
        puts "A configured DNS Server's IP Address is " + dns_server
    end
    puts "-----\n"

    # Check with user if they want to use these settings
    use_these_settings = ask("Are you sure you want to use these settings? (y/n)") { |q| q.echo = true }
    while !(use_these_settings == "n" or use_these_settings == "y")
        use_these_settings = ask("Invalid selection. Please enter y or n ::") { |q| q.echo = true }
    end

    return use_these_settings
end

def check_exit_without_saving()
    exit_without_saving = ask("Would you like to exit without saving changes? (y/n)") { |q| q.echo = true }

    while !(exit_without_saving == "n" or exit_without_saving == "y")
        exit_without_saving = ask("Invalid selection. Please enter y or n ::") { |q| q.echo = true }
    end

    return exit_without_saving
end

def get_current_ip()
    ip_addr = `ifconfig $nic_name | egrep -o "inet addr:[0-9\.]+" | awk -F: '{print $2}' | grep -v "127.0.0.1"`.chomp
    return ip_addr
end

# Proxy can be disabled but still have HTTP_PROXY and HTTPS_PROXY set
# If it's disabled with settings, it says it's disabled.
# It it's enabled, it will print the HTTP and HTTPS proxy settings, empty or not.
def create_proxy_settings_message()
    proxy_enabled= `grep PROXY_ENABLED /etc/sysconfig/proxy | awk -F'"' '{print $2}'`.chomp
    if proxy_enabled == "yes"
        configured_http_proxy = `egrep "^HTTP_PROXY" /etc/sysconfig/proxy | awk -F'"' '{print $2}'`.chomp
        configured_https_proxy = `egrep "^HTTPS_PROXY" /etc/sysconfig/proxy | awk -F'"' '{print $2}'`.chomp

        # You can enable the proxy without actually typing a proxy in...
        configured_proxy_message = ""
        if configured_http_proxy.empty?
            configured_proxy_message += "HTTP_PROXY was left blank.\n"
        else
            configured_proxy_message += "HTTP_PROXY is: " + configured_http_proxy + "\n"
        end

        if configured_https_proxy.empty? or configured_https_proxy == "https://"
            configured_proxy_message += "HTTPS_PROXY was left blank.\n"
        else
            configured_proxy_message += "HTTPS_PROXY is: " + configured_https_proxy + "\n"
        end

    else # Proxy is not configured
        configured_proxy_message = "Operations Manager was not configured to use a proxy"
    end
    return configured_proxy_message
end

# <------- MAIN -------->
if __FILE__ == $PROGRAM_NAME
    puts "Welcome to the VMTurbo Network Configuration Utility.\n\n"

    exit_without_saving = "n"

    # Prompt the user to choose DHCP or to use a Static IP
    static_or_dhcp = choose_if_static_or_dhcp()
    if static_or_dhcp == "dhcp"
        write_dhcp_settings_to_config_file()

    else # Configure Static IP
        use_these_settings = "n"
        ip_address, netmask, gateway, dns = "", "", "", ""

        while use_these_settings == "n"
            ip_address, netmask, gateway, dns = collect_static_ip_settings()
            use_these_settings = confirm_settings_with_user(ip_address, netmask, gateway, dns)

            if use_these_settings == "n"
                exit_without_saving = check_exit_without_saving()
            end

            if exit_without_saving == "y"
                break
            end
        end # Done checking user input for settings and whether they want to use or discard them

        if exit_without_saving == "y"
            puts "Exiting without changing IP Address configuration."
        else #
            write_static_ip_to_config_file(ip_address, netmask, gateway)
            set_default_routing_table(gateway)
            save_dns_servers_to_config_file(dns)
        end
    end # Done setting static IP information

    # During static IP configuration, the user MIGHT specify that they want to exit without saving
    # If they did NOT choose that, then let's set the proxy and restart network + welcome services
    if exit_without_saving == "n"
        # Set Proxy information using yast then restart networking to make changes take effect
        puts "Opening yast to set the Proxy Settings\n"
        set_proxy = `yast proxy`

        puts "Restarting networking to make changes take effect. This may take up to 60 seconds.\n\n"
        restart_network = `service network restart`

        # Restarting the welcome service causes the text at the VM Console to get updated with new IP
        welcome_service = `service welcome restart`
    end

    # Get current settings
    configured_ip = get_current_ip()
    configured_proxy_message = create_proxy_settings_message()

    puts "Operations Manager is now running with IP: " + configured_ip
    puts configured_proxy_message + "\n\n"
    puts "Goodbye!"
end
