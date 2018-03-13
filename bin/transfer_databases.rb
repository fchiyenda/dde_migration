def get_database_names
databases = ActiveRecord::Base.connection.select_all <<EOF
	show databases like '%openmrs%';
EOF

	names = []
	(databases || []).each	do |n|
		names << n["Database (%openmrs%)"]
		puts names.last

		#push copy data tables to 12.70

		puts "Create database in remote server"
		#create database if it does not exist
    `mysql -h192.168.12.70 -utest -ptest -e "CREATE database IF NOT EXISTS #{names.last};"`
    
    puts "processing data for #{names.last}"

		`mysqldump -h192.168.5.90 -uroot -proot "#{names.last}" --tables person person_name person_attribute person_address patient_identifier | mysql -h192.168.12.70 -utest -ptest "#{names.last}" --verbose`
	end
end

get_database_names