def database_names
  databases = ActiveRecord::Base.connection.select_all <<EOF
	show databases like '%openmrs%';
EOF

  names = []
  (databases || []).each	do |n|
    names << n['Database (%openmrs%)']
    puts names.last
  end
  return names
end

def site_prefix(databasename)
  prefix = ActiveRecord::Base.connection.select_one <<EOF
  select property_value from #{databasename}.global_property where property = 'site_prefix';
EOF
end

def start
  @config = YAML.load_file('config/database.yml')
  names = database_names
  names.sort.each do |databasename|
    begin
    site_code = site_prefix(databasename)
    puts "Getting demographics from mysql #{databasename} .. #{site_code['property_value']}"
    `mysqldump -h #{@config['development']['host']} -u#{@config['development']['username']} -p#{@config['development']['password']} #{databasename} --tables person person_name person_address person_attribute patient_identifier person_attribute_type patient_identifier_type global_property > #{Rails.root}/log/openmrs_#{site_code['property_value']}.sql`
    rescue Exception => e
      `echo "#{e}" >> #{Rails.root}/log/error.log`
      next
    end
  end 
end
start
