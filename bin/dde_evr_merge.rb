require 'rest-client'
require 'json'
require 'mysql2'
require 'damerau-levenshtein'

@data = ''
@i = 1

=begin
def dbconnect(host,user,pwd)
   @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}")
  end

def querydb(seqel)
   @rs = @cnn.query("#{seqel}")
end
  
def connect_to_mysqldb(h,u,p)
  dbconnect("#{h}","#{u}","#{p}")
end

def get_evr_data(skip = 0)
  url = "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}"\
  "/#{@config['evr']['couchdb']}/_all_docs?include_docs=true&skip="\
  "#{skip}&limit=10000"
  JSON.parse(RestClient.get(url, content_type: :json))['rows']
end

=end


def insert_into_person_merge(person,source)
		ActiveRecord::Base.connection.execute <<EOF
	      INSERT INTO evr_merge_person
	      VALUES(DEFAULT,
	       NULL,
	      '#{person['identifier']}',
	      '#{source}',
	      '#{person['given_name']}',
	      '#{person['middle_name']}',
	      '#{person['family_name']}',
	      '#{person['gender']}',
	      '#{person['birthdate']}',
	      '#{person['birthdate_estimated']}',
	      '#{person['closest_landmark']}',
	      '#{person['current_residence']}',
	      '#{person['current_village']}',
	      '#{person['current_ta']}',
	      '#{person['current_district']}',
	      '#{person['home_village']}',
	      '#{person['home_ta']}',
	      '#{person['home_district']}',
	      '#{person['country_of_residence']}',
	      '#{person['citizenship']}',
	      '#{person['occupation']}',
	      '#{person['home_phone_number']}',
	      '#{person['cell_phone_number']}',
	      '#{person['office_phone_number']}',
	      '#{person['created_at']}',
	      '#{person['assigned_site']}');
EOF

raise person.inspect

end

def update_person_merge_conflict(person, merge_id, merge_reason,source)
	begin
		ActiveRecord::Base.connection.execute <<EOF
	      INSERT INTO evr_person_merge_conflict
	      VALUES(DEFAULT,
	      	'#{person['identifier']}',
	      	'#{source}',
	      	'#{merge_id.to_i}',
	      	'#{merge_reason.to_i}',
	      	'#{person['given_name']}',
	      	'#{person['middle_name']}',
	      	'#{person['family_name']}',
	      	'#{person['gender']}',
	      	'#{person['birthdate']}',
	      	'#{person['birthdate_estimated']}',
	      	'#{person['closest_landmark']}',
	      	'#{person['current_residence']}',
	      	'#{person['current_village']}',
	      	'#{person['current_ta']}',
	      	'#{person['current_district']}',
	      	'#{person['home_village']}',
	      	'#{person['home_ta']}',
	      	'#{person['home_district']}',
	      	'#{person['country_of_residence']}',
	      	'#{person['citizenship']}',
	      	'#{person['occupation']}',
	      	'#{person['home_phone_number']}',
	      	'#{person['cell_phone_number']}',
	      	'#{person['office_phone_number']}',
	      	'#{person['created_at']}',
	      	'#{person['assigned_site']}');
EOF

	rescue StandardError => e
    `echo "#{e}" >> #{Rails.root}/log/dde_evr_merge_error.log`
  end
end

def escape_apostrophes(person)

	person['given_name'].to_s.gsub!("'","''")
	person['middle_name'].to_s.gsub!("'","''")
	person['family_name'].to_s.gsub!("'","''")
	person['closest_landmark'].to_s.gsub!("'","''")
	person['current_residence'].to_s.gsub!("'","''")
	person['current_village'].to_s.gsub!("'","''")
	person['current_district'].to_s.gsub!("'","''")
	person['current_ta'].to_s.gsub!("'","''")
	person['home_village'].to_s.gsub!("'","''")
	person['home_district'].to_s.gsub!("'","''")
	person['home_ta'].to_s.gsub!("'","''")

  return person
end

def scenario1(person)
ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['home_district']}')
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}')
				AND lower(current_district) = lower('#{person['current_district']}')
				AND lower(current_ta) = lower('#{person['current_ta']}')
				AND lower(current_village) = lower('#{person['current_village']}');
EOF
end

def scenario2(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['home_district']}')
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}')
				AND lower(current_district) = lower('#{person['current_district']}')
				AND lower(current_ta) = lower('#{person['current_ta']}')
				AND lower(current_village) = lower('#{person['current_village']}');
EOF
end

def scenario3(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['home_district']}')
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}');
EOF
end

def scenario4(person)
  ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['home_district']}')
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}');
EOF
end

def scenario5(person)
  ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}')
				AND lower(current_ta) = lower('#{person['current_ta']}')
				AND lower(current_village) = lower('#{person['current_village']}');
EOF
end

def scenario6(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}')
				AND lower(current_ta) = lower('#{person['current_ta']}')
				AND lower(current_village) = lower('#{person['current_village']}');
EOF
end

def scenario7(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}');
EOF
end

def scenario8(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}');
EOF
end

def scenario9(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}';
EOF
end

def scenario10(person)
ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['home_ta']}')
				AND lower(home_village) = lower('#{person['home_village']}');
EOF
end

def scenario11(person)
ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['given_name']}')
				AND lower(middle_name) = lower('#{person['middle_name']}')
				AND lower(family_name) = lower('#{person['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(current_district) = lower('#{person['current_district']}')
				AND lower(current_ta) = lower('#{person['current_ta']}');
EOF
end

def check_against_merge_criteria(person,source)
	person = update_person_dob(person)
	if !scenario1(person).blank?
    update_person_merge_conflict(person, scenario1(person).first['merge_id'], '1', source)
  elsif !scenario2(person).blank?
    update_person_merge_conflict(person, scenario2(person).first['merge_id'], '2', source)
  elsif !scenario3(person).blank?
    update_person_merge_conflict(person, scenario3(person).first['merge_id'], '3', source)
  elsif !scenario4(person).blank?
    update_person_merge_conflict(person, scenario4(person).first['merge_id'], '4', source)
  elsif !scenario5(person).blank?
    update_person_merge_conflict(person, scenario5(person).first['merge_id'], '5', source)
  elsif !scenario6(person).blank?
    update_person_merge_conflict(person, scenario6(person).first['merge_id'], '6', source)
  elsif !scenario7(person).blank?
    update_person_merge_conflict(person, scenario7(person).first['merge_id'], '7', source)
  elsif !scenario8(person).blank?
    update_person_merge_conflict(person, scenario8(person).first['merge_id'], '8', source)
  elsif !scenario9(person).blank?
    update_person_merge_conflict(person, scenario9(person).first['merge_id'], '9', source)
  elsif !scenario10(person).blank?
    update_person_merge_conflict(person, scenario10(person).first['merge_id'], '10', source)
  elsif !scenario11(person).blank?
    update_person_merge_conflict(person, scenario11(person).first['merge_id'], '11', source)
  else
  	insert_into_person_merge(person, source)
  end
end

def update_person_dob(person)
#Check if day and month are both estimated
	if person['birthdate'].blank? || !person.key?('birthdate')
		check_date = ["1900","01","01"]
	else
		if person['birthdate'].include?('-') 
			check_date = person['birthdate'].split('-')
	  elsif person['birthdate'].include?('/')
			check_date = person['birthdate'].split('/')
		end 

		if check_date[0].include?('?') && check_date[1].include?('?')
			check_date[0] = '01'
			check_date[1] = '07'
		elsif check_date[0].include?('?')
			check_date[0] = '15'
	  elsif check_date[2].include?('?')
	  	check_date[0] = '01'
			check_date[1] = '01'
	  	check_date[2] = '1900'
	  else
	  	#Do nothing
	  end
	end
	i = 0
	check_date.each do |value|
		check_date[i] = '1900' if value == '0000'
		check_date[i] = '01' if value == '00'
		check_date[i] = '01' if value == '00'
		i += 1
	end
	dob = "#{check_date[2]}-#{check_date[1]}-#{check_date[0]}"

	person['birthdate'] = dob.to_date.strftime("%Y-%m-%d")
	return person
end

def convert_birthdate_estimated_to_boolean(person)
	
	if person['birthdate_estimated'].to_s == '1'
	  dob_est = true
	  person['birthdate_estimated'] = dob_est
	elsif person['birthdate_estimated'].to_s == '0'
	  dob_est = false
	  person['birthdate_estimated'] = dob_est
	else
		person['birthdate_estimated'] = false
	end

	return person
end

def write_data_to_file(couch_data)
	couch_data.each do |person|
		puts "Processing ... #{person['doc']['_id']}"
    next unless person['doc']['type'] == 'Person' 
=begin
    &&
                person['doc']['addresses']['current_ta'] == 'Mtema' &&
                person['doc']['addresses']['current_district'] == 'Lilongwe'
=end
    person = convert_birthdate_estimated_to_boolean(person)
		person = escape_apostrophes(person)
		person = update_person_dob(person['doc'])
    @data += "(DEFAULT," +
	      		"NULL," +
	          "'#{person['_id']}'," +
	      		"'EVR'," +
	      		"'#{person['given_name']}'," +
	      		"'#{person['middle_name']}'," +
	      		"'#{person['family_name']}'," +
	      		"'#{person['gender']}'," +
	      		"'#{person['birthdate']}'," +
	      		"'#{person['birthdate_estimated']}'," +
	      		"'#{person['closest_landmark']}'," +
	      		"'#{person['current_residence']}'," +
	      		"'#{person['current_village']}'," +
	      		"'#{person['current_ta']}'," +
	      		"'#{person['current_district']}'," +
	      		"'#{person['home_village']}'," +
	      		"'#{person['home_ta']}'," +
	      		"'#{person['home_district']}'," +
	      		"'#{person['country_of_residence']}'," +
	      		"'#{person['citizenship']}'," +
	      		"'#{person['occupation']}'," +
	      		"'#{person['home_phone_number']}'," +
	      		"'#{person['cell_phone_number']}'," +
	      		"'#{person['office_phone_number']}'," +
	      		"'#{person['created_at']}'," +
	      		"'#{persons['assigned_site']}'),"

    if (@i % 100).zero? || @i  == @number_of_records
    	@data.chomp!(',')
    puts 'Saving data to Postgre database'
    	ActiveRecord::Base.connection.execute <<EOF
    	INSERT INTO evr_person values #{@data}
EOF
		@data.clear
		end
		#insert_into_person_merge(data)
		# check_against_merge_criteria(person['doc'],"Mtema")
		@i += 1
  end
end

def create_table
  ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS evr_merge_person;
EOF

  ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS evr_person_merge_conflict;
EOF

ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE evr_merge_person(
   merge_id SERIAL,
   person_id INT NULL,
   identifier VARCHAR(255) NULL,
   source VARCHAR(255) NULL,
   given_name VARCHAR(255) NULL,
   middle_name VARCHAR(255) NULL,
  family_name VARCHAR(255) NULL,
  gender VARCHAR(255) NULL,
  dob DATE NULL,
  dob_estimated BOOLEAN NULL,
  closest_landmark VARCHAR(255) NULL,
  current_residence VARCHAR(255) NULL,
  current_village VARCHAR(255) NULL,
  current_ta VARCHAR(255) NULL,
  current_district VARCHAR(255) NULL,
  home_village VARCHAR(255) NULL,
  home_ta VARCHAR(255) NULL,
  home_district VARCHAR(255) NULL,
  country_of_residence VARCHAR(255) NULL,
  citizenship VARCHAR(255) NULL,
  occupation VARCHAR(255) NULL,
  home_phone_number VARCHAR(255) NULL,
  cell_phone_number VARCHAR(255) NULL,
  office_phone_number VARCHAR(255) NULL,
  created_at VARCHAR(255) NULL,
  assigned_site VARCHAR(255) NULL,  
  PRIMARY KEY (merge_id));
EOF

ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE evr_person_merge_conflict(
  merge_conflict_id SERIAL,
  identifier VARCHAR(255) NULL,
  source VARCHAR(255) NULL,
  merge_id INT NOT NULL,
  merge_reason INT NOT NULL,
  given_name VARCHAR(255) NULL,
  middle_name VARCHAR(255) NULL,
  family_name VARCHAR(255) NULL,
  gender VARCHAR(255) NULL,
  dob DATE NULL,
  dob_estimated BOOLEAN NULL,
  closest_landmark VARCHAR(255) NULL,
  current_residence VARCHAR(255) NULL,
  current_village VARCHAR(255) NULL,
  current_ta VARCHAR(255) NULL,
  current_district VARCHAR(255) NULL,
  home_village VARCHAR(255) NULL,
  home_ta VARCHAR(255) NULL,
  home_district VARCHAR(255) NULL,
  country_of_residence VARCHAR(255) NULL,
  citizenship VARCHAR(255) NULL,
  occupation VARCHAR(255) NULL,
  home_phone_number VARCHAR(255) NULL,
  cell_phone_number VARCHAR(255) NULL,
  office_phone_number VARCHAR(255) NULL,
  created_at VARCHAR(255) NULL,
  assigned_site VARCHAR(255) NULL,  
  PRIMARY KEY (merge_conflict_id));
EOF

end

=begin
# This is cleaning code uncomment code above for merging code
  `rm log/cleaning_progress.log`
  ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS evr_person;
EOF

  ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS evr_person_conflict;
EOF

ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE evr_person(
   merge_id SERIAL,
   person_id INT NULL,
   identifier VARCHAR(255) NULL,
   source VARCHAR(255) NULL,
   given_name VARCHAR(255) NULL,
   middle_name VARCHAR(255) NULL,
  family_name VARCHAR(255) NULL,
  gender VARCHAR(255) NULL,
  dob DATE NULL,
  dob_estimated BOOLEAN NULL,
  closest_landmark VARCHAR(255) NULL,
  current_residence VARCHAR(255) NULL,
  current_village VARCHAR(255) NULL,
  current_ta VARCHAR(255) NULL,
  current_district VARCHAR(255) NULL,
  home_village VARCHAR(255) NULL,
  home_ta VARCHAR(255) NULL,
  home_district VARCHAR(255) NULL,
  country_of_residence VARCHAR(255) NULL,
  citizenship VARCHAR(255) NULL,
  occupation VARCHAR(255) NULL,
  home_phone_number VARCHAR(255) NULL,
  cell_phone_number VARCHAR(255) NULL,
  office_phone_number VARCHAR(255) NULL,
  created_at VARCHAR(255) NULL,
  assigned_site VARCHAR(255) NULL,
  PRIMARY KEY (merge_id));
EOF

ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE evr_person_conflict(
  merge_conflict_id SERIAL,
  person_id INT NULL,
  identifier VARCHAR(255) NULL,
  source VARCHAR(255) NULL,
  merge_id INT NOT NULL,
  merge_conflict_id_ref INT NOT NULL,
  conflict_reason INT NOT NULL,
  given_name VARCHAR(255) NULL,
  middle_name VARCHAR(255) NULL,
  family_name VARCHAR(255) NULL,
  gender VARCHAR(255) NULL,
  dob DATE NULL,
  dob_estimated BOOLEAN NULL,
  closest_landmark VARCHAR(255) NULL,
  current_residence VARCHAR(255) NULL,
  current_village VARCHAR(255) NULL,
  current_ta VARCHAR(255) NULL,
  current_district VARCHAR(255) NULL,
  home_village VARCHAR(255) NULL,
  home_ta VARCHAR(255) NULL,
  home_district VARCHAR(255) NULL,
  country_of_residence VARCHAR(255) NULL,
  citizenship VARCHAR(255) NULL,
  occupation VARCHAR(255) NULL,
  home_phone_number VARCHAR(255) NULL,
  cell_phone_number VARCHAR(255) NULL,
  office_phone_number VARCHAR(255) NULL,
  created_at VARCHAR(255) NULL,
  assigned_site VARCHAR(255) NULL,
  PRIMARY KEY (merge_conflict_id));

EOF


=end

def query_db(query)
	
	data = ActiveRecord::Base.connection.select_all <<EOF
	 	 	 #{query};
EOF

return data

end

def get_data
	  evr_record_count = ActiveRecord::Base.connection.select_all <<EOF
	 	 	 select count(*) from evr_person;
EOF

	i = 0
	while i <= evr_record_count.first['count'].to_i do
		person = query_db("select * from evr_person limit 100 offset #{i}")
		person.each do |p|
				p = convert_birthdate_estimated_to_boolean(p)
				p = escape_apostrophes(p)
				check_against_merge_criteria(p,'evr')
		end
		i += 100
	end
	  
	  ngoni_record_count = ActiveRecord::Base.connection.select_all <<EOF
	    select count(*) from ngoni_person;
EOF

	i = 0
	while i <= ngoni_record_count.first['count'].to_i do
		person = query_db("select * from ngoni_person limit 100 offset #{i}")
		person.each do |p|	
				p = convert_birthdate_estimated_to_boolean(p)
				p = escape_apostrophes(p)
				check_against_merge_criteria(p,'ngoni')
		end
		i += 100
	end
end

def start
  @config = YAML.load_file('config/database.yml')
	puts "Create table in database"
  create_table
=begin

	url = "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}/#{@config['evr']['couchdb']}/_all_docs?limit=1"
	@number_of_records = JSON.parse(RestClient.get(url,content_type: :json))['total_rows'].to_i	
	counter = 0
	while counter <= @number_of_records
  puts "Getting data from couchdb from #{counter} to #{counter + 10_000}"
   write_data_to_file(get_evr_data(counter))
  	counter += 10_000
 	end
=end
	
# Getting data from MySQL Ngoni database
 puts "Getting data from MySQL"
 get_data
end

start