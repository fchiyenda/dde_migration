@start_at = Time.now()
@version_4_identifiers = {}
@dde_person_primary_id = 0

(['dde_duplicates.log','dde_migration_error.log','dde_person_update.sql','dde_person.sql','dde_person_identifiers.sql']).each do |dde_file|
  dde_person_file = "#{Rails.root}/log/#{dde_file}"
  File.open(dde_person_file, 'w+') { |file| file.write("") }
end

def create_tables

	ActiveRecord::Base.connection.execute <<EOF
      DROP DATABASE IF EXISTS `dde_migration_master`;
EOF

ActiveRecord::Base.connection.execute <<EOF
      CREATE DATABASE `dde_migration_master`;
EOF

  ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS `dde_migration_master.dde_person`;
EOF

 ActiveRecord::Base.connection.execute <<EOF
      CREATE TABLE dde_migration_master.dde_person(`migration_patient_id` INT NOT NULL AUTO_INCREMENT,
  `given_name` VARCHAR(45) NULL,
  `middle_name` VARCHAR(45) NULL,
  `family_name` VARCHAR(45) NULL,
  `gender` VARCHAR(6) NULL,
  `dob` DATE NULL,
  `dob_estimated` BINARY NULL,
  `death_date` DATE NULL,
  `home_district` VARCHAR(45) NULL,
  `home_ta` VARCHAR(245) NULL,
  `home_village` VARCHAR(245) NULL,
  `current_district` VARCHAR(45) NULL,
  `current_ta` VARCHAR(245) NULL,
  `current_village` VARCHAR(245) NULL,
  `primary_identifier` VARCHAR(45) NULL,
  `location_id` VARCHAR(45) NOT NULL,
  `date_created` TIMESTAMP NULL,
  PRIMARY KEY (`migration_patient_id`),
  UNIQUE INDEX `primary_identifier_UNIQUE` (`primary_identifier` ASC));
EOF

ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS `dde_migration_master.dde_person_identifiers`;
EOF

 ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE dde_migration_master.dde_person_identifiers(`dde_person_identifiers_id` INT NOT NULL AUTO_INCREMENT,
  `migration_patient_id` INT NOT NULL,
  `identifier` VARCHAR(45) NOT NULL,
  `timestamp` TIMESTAMP NULL,
  PRIMARY KEY (`dde_person_identifiers_id`));
EOF

end

def get_database_names
	databases = ActiveRecord::Base.connection.select_all <<EOF
	show databases like '%openmrs%';
EOF

	names = []
	(databases || []).each	do |n|
		names << n["Database (%openmrs%)"]
		puts names.last
	end

	return names
end

def get_version4_patient_ids(databasename)
	begin 
	patient_ids = ActiveRecord::Base.connection.select_all <<EOF
	SELECT patient_id FROM #{databasename}.patient_identifier where length(identifier) = 6 
	AND voided = 0 and identifier_type = 3 group by patient_id ,identifier;
EOF

	rescue 
		puts "Table does not exist in #{databasename}"
	end
	
	version4_ids = []

	(patient_ids || []).each do |v4|
		version4_ids << v4['patient_id'].to_i
	end

	return version4_ids

end

def start
	#create_tables
	names = get_database_names
	names.sort.each do |databasename|
		patient_ids = get_version4_patient_ids(databasename)
		puts "#{patient_ids.length}  #{databasename}"
		create_migrate_to_dde_flattables(patient_ids,databasename) unless patient_ids.blank?
	end

  (@version_4_identifiers || {}).each do |version4_id, dde_person_id|
    update_string =<<EOF
    UPDATE dde_person SET primary_identifier = "#{version4_id}" WHERE migration_patient_id = #{dde_person_id};
EOF

    dde_person_file = "#{Rails.root}/log/dde_person_update.sql"
    File.open(dde_person_file, 'a') { |file| file.write(update_string) }
  end


  puts "Script done: start at: #{@start_at.strftime('%d/%b/%Y %H:%M:%S')}, ended at: #{Time.now().strftime('%d/%b/%Y %H:%M:%S')}"
end

def create_migrate_to_dde_flattables(patient_ids,databasename)
begin
  location = ActiveRecord::Base.connection.select_one <<EOF
  SELECT property_value FROM #{databasename}.global_property WHERE property = 'current_health_center_id';
EOF
rescue
   location_id = location['property_value'].to_i rescue 0
end
	begin
	patient_ids.each do |patient_id|
		person = ActiveRecord::Base.connection.select_one <<EOF
			SELECT gender,birthdate,birthdate_estimated,death_date FROM #{databasename}.person WHERE person_id = #{patient_id} LIMIT 1;
EOF

person_name = ActiveRecord::Base.connection.select_one <<EOF
			SELECT given_name,middle_name,family_name FROM #{databasename}.person_name WHERE person_id = #{patient_id} 
			AND voided = 0 ORDER BY date_created desc LIMIT 1;
EOF

patient_identifiers = ActiveRecord::Base.connection.select_all <<EOF
			SELECT identifier,identifier_type,date_created FROM #{databasename}.patient_identifier 
			WHERE patient_id = #{patient_id} 
			AND voided = 0 
			AND identifier_type IN(2,3) 
			ORDER BY date_created desc;
EOF

person_address = ActiveRecord::Base.connection.select_one <<EOF
			SELECT address1,address2,city_village,state_province,postal_code,country,county_district,neighborhood_cell,township_division FROM #{databasename}.person_address WHERE person_id = #{patient_id} 
			AND voided = 0 ORDER BY date_created desc LIMIT 1;
EOF

  @dde_person_primary_id += 1
  insert_into_tables(person,person_name,patient_identifiers,person_address,databasename, location_id)
	end
	rescue
		`echo "Could not find one of the Tables: #{databasename}" >> #{Rails.root}/log/dde_migration_error.log`
	end
end

def insert_into_tables(person,person_name,patient_identifiers,person_address, dname, location_id)
	primary_identifier = nil 
	identifiers = []

	(patient_identifiers || []).each do |i|
		if i['identifier'].length == 6 and i['identifier_type'].to_i == 3
			primary_identifier = i['identifier'].squish
		else
			identifiers << i['identifier'].squish
		end
	end

  if @version_4_identifiers[primary_identifier].blank?
    @version_4_identifiers[primary_identifier] = @dde_person_primary_id
  else
    created_id = @version_4_identifiers[primary_identifier] 
    created_rec =<<EOF
    NULL, #{created_id},"#{primary_identifier}","#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"
EOF

    ################################################################
    `echo '#{created_rec}' >> #{Rails.root}/log/dde_person_identifiers.sql`
    identifiers << primary_identifier

    (@version_4_identifiers.keys || []).each_with_index do |(k, patient_id), i|
      if k == primary_identifier
        puts "--------------------------------------------------- #{k}"
        dde_duplicate =<<EOF
        #{@version_4_identifiers[k]} :::: #{primary_identifier}
EOF

        dde_person_file = "#{Rails.root}/log/dde_duplicates.log"
        File.open(dde_person_file, 'a') { |file| file.write(dde_duplicate) }
	@version_4_identifiers.reject! {|key, value| key == primary_identifier}
      end
    end
    ################################################################


  end

  birthdate = person['birthdate'].to_date rescue nil
  if birthdate.blank?
    birthdate = "NULL" 
  else  
    birthdate = '"' + "#{birthdate}" + '"'
  end


  death_date = person['death_date'].to_date rescue nil
  if death_date.blank?
    death_date = "NULL" 
  else  
    death_date = '"' + "#{death_date}" + '"' 
  end

  middle_name = person_name['middle_name'].to_s rescue nil
  if middle_name.blank?
    middle_name = "NULL" 
  else  
    middle_name = '"' + "#{middle_name}" + '"' 
  end


  family_name = person_name['family_name'].to_s rescue nil
  if family_name.blank?
    family_name = "NULL" 
  else  
    family_name = '"' + "#{family_name}" + '"' 
  end

  given_name = person_name['given_name'].to_s rescue nil
  if given_name.blank?
    given_name = "NULL" 
  else  
    given_name = '"' + "#{given_name}" + '"' 
  end

  gender = person['gender'].to_s rescue nil
  if gender.blank?
    gender = "NULL" 
  else  
    gender = '"' + "#{gender}" + '"' 
  end

  birthdate_estimated = person['birthdate_estimated'].to_i rescue 1

  address2 = person_address['address2'].to_s rescue nil
  if address2.blank?
    address2 = "NULL" 
  else  
    address2= '"' + "#{address2}" + '"' 
  end

  city_village = person_address['city_village'].to_s rescue nil
  if city_village.blank?
    city_village = "NULL" 
  else  
    city_village = '"' + "#{city_village}" + '"' 
  end


  state_province = person_address['state_province'].to_s rescue nil
  if state_province.blank?
    state_province = "NULL" 
  else  
    state_province = '"' + "#{state_province}" + '"' 
  end

  county_district = person_address['county_district'].to_s rescue nil
  if county_district.blank?
    county_district = "NULL" 
  else  
    county_district = '"' + "#{county_district}" + '"' 
  end

  neighborhood_cell = person_address['neighborhood_cell'].to_s rescue nil
  if neighborhood_cell.blank?
    neighborhood_cell = "NULL" 
  else  
    neighborhood_cell = '"' + "#{neighborhood_cell}" + '"' 
  end

  township_division = person_address['township_division'].to_s rescue nil
  if township_division.blank?
    township_division = "NULL" 
  else  
    township_division = '"' + "#{township_division}" + '"' 
  end

  dde_person =<<EOF
		#{@dde_person_primary_id}, #{given_name},#{middle_name},#{family_name},#{gender},#{birthdate},#{birthdate_estimated},#{death_date},#{address2},#{city_village},#{state_province},#{county_district},#{neighborhood_cell},#{township_division},NULL,#{location_id},"#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"
EOF

  begin
    dde_person_file = "#{Rails.root}/log/dde_person.sql"
    File.open(dde_person_file, 'a') { |file| file.write(dde_person) }
  rescue
    `echo '#{dde_person}' >> #{Rails.root}/log/dde_migration_error.log`
  end

  dde_person_identifiers = ''
  (identifiers || []).each_with_index do |identifier, i|
  	dde_person_identifiers +=<<EOF
  	  NULL, #{@dde_person_primary_id},"#{identifier}", "#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"
EOF
		
  end
  
  unless dde_person_identifiers.blank?
    begin
      `echo '#{dde_person_identifiers}' >> #{Rails.root}/log/dde_person_identifiers.sql`
    rescue
      `echo '#{dde_person_identifiers}' >> #{Rails.root}/log/dde_person_identifiers.sql`
    end
  end

  puts "#{dname}...................... #{@dde_person_primary_id}"
end

start
