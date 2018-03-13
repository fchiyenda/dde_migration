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
  `home_ta` VARCHAR(45) NULL,
  `home_village` VARCHAR(45) NULL,
  `current_district` VARCHAR(45) NULL,
  `current_ta` VARCHAR(45) NULL,
  `current_village` VARCHAR(45) NULL,
  `primary_identifier` VARCHAR(45) NULL,
  `location_id` VARCHAR(45) NOT NULL,
  `date_created` TIMESTAMP NULL,
  PRIMARY KEY (`migration_patient_id`));
EOF

ActiveRecord::Base.connection.execute <<EOF
    ALTER TABLE dde_migration_master.dde_person ADD INDEX(`migration_patient_id`);
EOF

ActiveRecord::Base.connection.execute <<EOF
    ALTER TABLE dde_migration_master.dde_person ADD INDEX(`primary_identifier`);
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

ActiveRecord::Base.connection.execute <<EOF
    ALTER TABLE dde_migration_master.dde_person_identifiers ADD INDEX(`migration_patient_id`);
EOF

ActiveRecord::Base.connection.execute <<EOF
    ALTER TABLE dde_migration_master.dde_person_identifiers ADD INDEX(`identifier`);
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
	SELECT * FROM #{databasename}.patient_identifier where length(identifier) = 6 
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
	create_tables
	names = get_database_names
	count = []
	names.each do |databasename|
		count << databasename
		patient_ids = get_version4_patient_ids(databasename)
		puts "Processing database #{count.length} of #{names.length} #{patient_ids.length}  #{databasename}"
		create_migrate_to_dde_flattables(patient_ids,databasename) unless patient_ids.blank?
	end
end

def create_migrate_to_dde_flattables(patient_ids,databasename)

	begin
	patient_ids.each do |patient_id|
		person = ActiveRecord::Base.connection.select_one <<EOF
			SELECT * FROM #{databasename}.person WHERE person_id = #{patient_id} LIMIT 1;
EOF

person_name = ActiveRecord::Base.connection.select_one <<EOF
			SELECT * FROM #{databasename}.person_name WHERE person_id = #{patient_id} 
			AND voided = 0 ORDER BY date_created desc LIMIT 1;
EOF

patient_identifiers = ActiveRecord::Base.connection.select_all <<EOF
			SELECT * FROM #{databasename}.patient_identifier 
			WHERE patient_id = #{patient_id} 
			AND voided = 0 
			AND identifier_type IN(2,3) 
			ORDER BY date_created desc;
EOF

person_address = ActiveRecord::Base.connection.select_one <<EOF
			SELECT * FROM #{databasename}.person_address WHERE person_id = #{patient_id} 
			AND voided = 0 ORDER BY date_created desc LIMIT 1;
EOF
  
  insert_into_tables(person,person_name,patient_identifiers,person_address,databasename)
	end
	rescue
		`echo "Could not find one of the Tables" >> #{Rails.root}/log/dde_migration_error.log`
	end
end

def insert_into_tables(person,person_name,patient_identifiers,person_address, dname)
	primary_identifier = nil 
	location_id = nil 
	identifiers = []
	
	(patient_identifiers || []).each do |i|
		if i['identifier'].length == 6 and i['identifier_type'].to_i == 3
			primary_identifier = i['identifier']
		else
			identifiers << i['identifier']
		end
	end

	exist = ActiveRecord::Base.connection.select_one <<EOF
	SELECT * FROM dde_migration_master.dde_person p 
	LEFT JOIN dde_migration_master.dde_person_identifiers i 
	ON p.migration_patient_id = i.migration_patient_id
	WHERE (primary_identifier = '#{primary_identifier}'
	OR identifier = '#{primary_identifier}') LIMIT 1;
EOF

unless exist.blank?
	records_to_void = ActiveRecord::Base.connection.select_all <<EOF
	SELECT * FROM dde_migration_master.dde_person
	WHERE primary_identifier = "#{primary_identifier}";
EOF

  (records_to_void || []).each do |r|
  	ActiveRecord::Base.connection.execute <<EOF
  	INSERT INTO dde_migration_master.dde_person_identifiers
  	VALUES(null,#{r['migration_patient_id'].to_i},"#{primary_identifier}",
  		"#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}")
EOF

  end
 

	ActiveRecord::Base.connection.execute <<EOF
	UPDATE dde_migration_master.dde_person
	SET primary_identifier = NULL
	WHERE primary_identifier = "#{primary_identifier}"
EOF

    identifiers << primary_identifier
		primary_identifier = "NULL"
else
	primary_identifier = "'#{primary_identifier}'"
end

  death_date = person['death_date'].to_date rescue nil
  death_date = "'#{death_date}'" unless death_date.blank?
  death_date = "NULL" if death_date.blank?

	dde_person = ActiveRecord::Base.connection.execute <<EOF
		INSERT INTO dde_migration_master.dde_person
		VALUES(NULL,"#{person_name['given_name']}",
			"#{person_name['middle_name']}",
			"#{person_name['family_name']}",
			"#{person['gender']}",
			"#{person['birthdate']}",
			#{person['birthdate_estimated']},
			#{death_date},
			"#{person_address['address2']}",
			"#{person_address['city_village']}",
			"#{person_address['state_province']}",
			"#{person_address['county_district']}",
			"#{person_address['neighborhood_cell']}",
			"#{person_address['township_division']}",
			#{primary_identifier},
			 "#{location_id}",
			 "#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}")
EOF

dde_person = ActiveRecord::Base.connection.select_one <<EOF
	SELECT * FROM dde_migration_master.dde_person
	ORDER BY migration_patient_id DESC LIMIT 1;
EOF

  (identifiers || []).each do |i|
  	ActiveRecord::Base.connection.execute <<EOF
  	INSERT INTO dde_migration_master.dde_person_identifiers
  	VALUES(null,#{dde_person['migration_patient_id'].to_i},"#{i}",
  		"#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}")
EOF
  end

  puts ".... #{dname} created: #{dde_person['given_name']} #{dde_person['family_name']} #{dde_person['gender']} >>> #{dde_person['primary_identifier']}"
end

start
