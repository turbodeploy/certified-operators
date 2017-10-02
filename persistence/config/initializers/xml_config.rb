
require 'rbconfig'

begin
  if Config::CONFIG['host_os'].to_s[/mingw|mswin/]
    require 'nokogiri'
  else
    require 'libxml'
  end
rescue
end

if defined?(ActiveSupport::XmlMini)
  case
    when defined?(Nokogiri)
      ActiveSupport::XmlMini.backend = 'Nokogiri'
      #puts "Using Nokogiri for XML"

    when defined?(LibXML)
      ActiveSupport::XmlMini.backend = 'LibXML'
      #puts "Using LibXML for XML"

    else
      # there is no XML parser override - use the native parser
      #puts "Using default XML parser"
  end
end
