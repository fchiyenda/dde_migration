require 'yaml'

def uncompress(file)
  puts "Uncompressing #{file}"
  `gunzip #{file}`
end

def loaddumps(database_name,dumpfile_name)
puts "Creating database #{database_name}"
ActiveRecord::Base.connection.execute <<EOF
  DROP DATABASE IF EXISTS #{database_name[database_name.index('openmrs')..(database_name.length - 5)]};
EOF
  ActiveRecord::Base.connection.execute <<EOF
    CREATE DATABASE #{database_name[database_name.index('openmrs')..(database_name.length - 5)]};
EOF
  puts "Loading #{dumpfile_name}"
  `pv #{dumpfile_name} | mysql -u#{@dbusername} -p#{@dbpassword} \
  #{database_name[database_name.index('openmrs')..(database_name.length - 5)]}`
  unless $? == 0
     `echo "#{dumpfile_name.slice(dumpfile_name.rindex('/')+1..dumpfile_name.length)} did not complete loading" >> #{Rails.root}/log/dde_load_error.log`
   end
end

def clean_dashes(dumpfile)
  while dumpfile.include?('-')
    dumpfile.sub!('-', '_')
  end
  return dumpfile
end

def start
  `rm  #{Rails.root}/log/dde_load_error.log`
	config = YAML.load_file('config/database.yml')
  @dbusername = config['development']['username']
  @dbpassword = config['development']['password']
  dumps = []
  puts 'Please full path to where dumps are located'
  path_to_dumps = gets.chomp
  dumps = Dir["#{path_to_dumps}/openmrs*"]
  dumps.each do |file|
    uncompress(file)
  end
  dumps.clear
  dumps = Dir["#{path_to_dumps}/openmrs*"]
  dumps.each do |dumpfile|
    dumpfile_name = dumpfile.dup
    database_name = clean_dashes(dumpfile)
    loaddumps(database_name,dumpfile_name)
  end
end
start
