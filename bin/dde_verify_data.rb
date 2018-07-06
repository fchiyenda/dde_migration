puts 'Getting all databases'
database = ActiveRecord::Base.connection.execute <<EOF
	show databases like 'openmrs%'
EOF
databases =  []
database.each do |db|
	databases << db
end

databases.flatten!

databases.each do |db|
	puts "Checking #{db}"
	tables = ActiveRecord::Base.connection.execute <<EOF
	show tables in #{db};
EOF
   
	if tables.count.to_i < 7 
		`echo #{db} >> #{Rails.root}/log/dbs_with_probs.log`
	end
end