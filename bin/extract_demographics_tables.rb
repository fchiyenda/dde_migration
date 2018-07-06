require 'yaml'

def uncompress(file)
  puts "Uncompressing #{file}"
  `gunzip #{file}`
end

def extract_tables(dumpfile_name)
  puts "Extracting data from #{dumpfile_name.sub(".sql","")}.person"
  `sed -n -e '/DROP TABLE IF EXISTS.*\`person\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`

  puts "Extracting data from #{dumpfile_name.sub(".sql","")}.person_name"
  `sed -n -e '/DROP TABLE IF EXISTS.*\`person_name\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`

   puts "Extracting data from #{dumpfile_name.sub(".sql","")}.person_address"
  `sed -n -e '/DROP TABLE IF EXISTS.*\`person_address\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`

   puts "Extracting data from #{dumpfile_name.sub(".sql","")}.person_attribute"
  `sed -n -e '/DROP TABLE IF EXISTS.*\`person_attribute\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`

   puts "Extracting data from #{dumpfile_name.sub(".sql","")}.patient_identifier"
  `sed -n -e '/DROP TABLE IF EXISTS.*\`patient_identifier\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`

   puts "Extracting data from #{dumpfile_name.sub(".sql","")}.person_attribute_type"
  `sed -n -e '/DROP TABLE IF EXISTS.*\`person_attribute_type\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`

   puts "Extracting data from #{dumpfile_name.sub(".sql","")}.patient_identifier_type"
  `sed -n -e '/DROP TABLE IF EXISTS.*\`patient_identifier_type\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`

  puts "Extracting data from #{dumpfile_name.sub(".sql","")}.global_property"
  `sed -n -e '/CREATE TABLE.*\`global_property\`/,/UNLOCK TABLES;/p' #{dumpfile_name} >> #{dumpfile_name.sub(".sql","")}_demographics.sql`
end

def start
  `rm  #{Rails.root}/log/dde_load_error.log`
	config = YAML.load_file('config/database.yml')
  @dbusername = config['development']['username']
  @dbpassword = config['development']['password']
  dumps = []
  puts 'Please full path to where dumps are located'
  path_to_dumps = gets.chomp
  dumps = Dir["#{path_to_dumps}/*openmrs*"]
  dumps.each do |file|
    uncompress(file)
  end
  dumps.clear
  dumps = Dir["#{path_to_dumps}/*openmrs*"]
  dumps.each do |dumpfile|
    dumpfile_name = dumpfile.dup
    extract_tables(dumpfile_name)
  end
end
start
