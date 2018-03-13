require 'yaml'

def uncompress(file)
  puts "Uncompressing #{file}"
  `gunzip #{file}`
end

def loaddumps(dumpfile,dumpfile_name)
ActiveRecord::Base.connection.execute <<EOF
  DROP DATABASE IF EXISTS #{dumpfile[dumpfile.index('openmrs')..(dumpfile.length - 5)]};
EOF
  ActiveRecord::Base.connection.execute <<EOF
    CREATE DATABASE #{dumpfile[dumpfile.index('openmrs')..(dumpfile.length - 5)]};
  EOF
  puts "Loading #{dumpfile_name}"
  `pv #{dumpfile_name} | mysql -u#{@dbusername} -p#{@dbpassword} \
  #{dumpfile[dumpfile.index('openmrs')..(dumpfile.length - 5)]}`
  sleep 1
end

def clean_dashes(dumpfile)
  while dumpfile.include?('-')
    dumpfile.sub!('-', '_')
  end
  return dumpfile
end

def start
	config = YAML.load_file('config/database.yml')
  @dbusername = config['development']['username']
  @dbpassword = config['development']['password']
  dumps = []
  puts 'Please full path to where dumps are located'
  path_to_dumps = gets.chomp
  dumps = Dir["#{path_to_dumps}/*"]
  dumps.each do |file|
    uncompress(file)
  end
  dumps.clear
  dumps = Dir["#{path_to_dumps}/*"]
  dumps.each do |dumpfile|
    dumpfile_name = dumpfile.dup
    dumpfile = clean_dashes(dumpfile)
    loaddumps(dumpfile,dumpfile_name)
  end
end
start
