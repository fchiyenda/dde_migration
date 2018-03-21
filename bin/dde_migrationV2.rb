@start_at = Time.now()
@version_4_identifiers = {}
@dde_person_primary_id = 0
@duplicate_ids = []

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

def get_version4_patients(databasename)
 begin
  version4_patients = ActiveRecord::Base.connection.select_all <<EOF
                      SELECT pi.patient_id,pi.identifier,identifier_type,
                      (SELECT group_concat(pi2.identifier) legacy
                      FROM #{databasename}.patient_identifier pi2
                      WHERE pi2.identifier_type = 2
                      AND pi.patient_id = pi2.patient_id
                      GROUP BY patient_id) legacy_ids,
                      pi.date_created,given_name,middle_name,family_name,gender,birthdate,birthdate_estimated,death_date,pa.*
                      FROM #{databasename}.patient_identifier pi
                      JOIN #{databasename}.person p
                      ON pi.patient_id = p.person_id
                      JOIN #{databasename}.person_name pn
                      ON pi.patient_id = pn.person_id
                      LEFT JOIN #{databasename}.person_address pa
                      ON pi.patient_id = pa.person_id
                      where length(pi.identifier) = 6 
                      AND pi.voided = 0 and identifier_type = 3 
                      group by pi.patient_id,pi.identifier;
EOF
 rescue Exception => e
   `echo '#{e}' >> #{Rails.root}/log/dde_migration_error.log`
 end
 return version4_patients
end

def create_migrate_to_dde_flattables(patients, databasename)
  begin
    location = ActiveRecord::Base.connection.select_one <<EOF
    SELECT property_value FROM #{databasename}.global_property WHERE property = 'current_health_center_id';
EOF
   location_id = location['property_value'].to_i
  rescue Exception => e
   `echo '#{e}' >> #{Rails.root}/log/dde_migration_error.log`
    location_id = 0
  end
  patients.each do |patient|
    @dde_person_primary_id += 1
    insert_into_tables(patient,location_id,databasename)
  end
end

def insert_into_tables(person,location_id,dname)
  identifiers = []
  
  if @version_4_identifiers[person['identifier']].blank?
    @version_4_identifiers[person['identifier']] = @dde_person_primary_id
  else
    created_id = @version_4_identifiers[person['identifier']]
    created_rec = "Null," +
                  "\"#{created_id}\"," +
                  "\"#{person['identifier']}\"," +
                  "\"#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}\","

    ################################################################
    `echo '#{created_rec}' >> #{Rails.root}/log/dde_person_identifiers.sql`
    unless $? == 0
    `echo '#{dde_person}' >> #{Rails.root}/log/dde_migration_error.log`
    end

    identifiers << person['identifier']

    #Checking if identifier is a duplicate
    (@version_4_identifiers.keys || []).each do |k, i|
      if k == person['identifier']
        puts "#{person['identifier']}--------------------------------------------- #{k}"
        dde_duplicate = "#{@version_4_identifiers[k]} :::: #{person['identifier']}"
        `echo '#{dde_duplicate}..#{dname}' >> #{Rails.root}/log/dde_duplicates.log`
         #@version_4_identifiers.reject! { |key, value| key == primary_identifier}
         @duplicate_ids << person['identifier']
      end
    end
    ################################################################
  end

  if person['death_date'].blank?
    dod = person['death_date'].to_s
  else
    dod = person['death_date'].strftime("%Y-%m-%d")
  end

  dde_person = "#{@dde_person_primary_id}," +
             "\"#{person['identifier'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['given_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['middle_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['family_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['gender']}\"," +
             "\"#{person['birthdate']}\"," +
             "#{person['birthdate_estimated']}," +
             "\"#{dod}\"," +
             "\"#{person['address2'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['city_village'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['state_province'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['county_district'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['neighborhood_cell'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['township_division'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{person['legacy_ids'].to_s.gsub("'","\'\\\\\'\'")}\"," +
             "\"#{location_id}\"," +
             "\"#{person['date_created']}\""

  `echo '#{dde_person}' >> #{Rails.root}/log/dde_person.sql`
  unless $? == 0
    `echo '#{dde_person}' >> #{Rails.root}/log/dde_migration_error.log`
  end

  dde_person_identifiers = ''
  (identifiers || []).each do |identifier|
    dde_person_identifiers = "Null," +
                              "\"#{@dde_person_primary_id}\"," +
                              "\"#{identifier}\"," +
                              "\"#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}\""
    unless dde_person_identifiers.blank?
        `echo '#{dde_person_identifiers}' >> #{Rails.root}/log/dde_person_identifiers.sql`
      unless $? == 0
        `echo '#{dde_person_identifiers}' >> #{Rails.root}/log/dde_migration_error.log`
      end
    end
  end
  puts "#{dname}...................... #{@dde_person_primary_id}"
end

def start
	#create_tables
  `rm #{Rails.root}/log/dde_person_update.sql`
  `rm #{Rails.root}/log/dde_migration_error.log`
  `rm #{Rails.root}/log/dde_person_identifiers.sql`
  `rm #{Rails.root}/log/dde_person.sql`
  `rm #{Rails.root}/log/dde_duplicates.log`
  names = get_database_names
	names.sort.each do |databasename|
    puts "Getting data from mysql #{databasename}"
    patients = get_version4_patients(databasename)
    
    puts "#{patients.length}....#{databasename}" rescue nil

		create_migrate_to_dde_flattables(patients, databasename) unless patients.blank?
	end
  
  #remove duplicate ids from hash
  puts "Cleaning duplicated ids"
  @duplicate_ids.each do |id|
    @version_4_identifiers.reject! { |key, value| key == id}
  end

  (@version_4_identifiers || {}).each do |version4_id, dde_person_id|
    update_string = "UPDATE dde_person SET primary_identifier = #{version4_id}" +
                    " WHERE migration_patient_id = #{dde_person_id};"
    `echo '#{update_string}' >> #{Rails.root}/log/dde_person_update.sql`
  end
  puts "Script done: start at: #{@start_at.strftime('%d/%b/%Y %H:%M:%S')}, ended at: #{Time.now().strftime('%d/%b/%Y %H:%M:%S')}"
end
start
