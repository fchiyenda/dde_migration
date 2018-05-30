  
  %x(rm #{Rails.root}/log/dde_load_npids_errors.log)
  i = 1
  File.open('/home/fchiyenda/Documents/Boabab/DDE/dde_npid.sql', 'r').each_line do |line|
  	puts "Processing record #{i}"
    line = "#{line}"
    line = line.split(',')
    if line[4].to_i == 1
       line[4] = 't' 
    elsif line[4].to_i == 0
       line[4] == 'f'
    end
   line = "(DEFAULT,#{line[1]},'#{line[2]}',4,'#{line[4]}','#{line[5]}','#{line[6]}')"
   begin
      ActiveRecord::Base.connection.execute <<EOF
    	  INSERT INTO npids values #{line};
EOF

    rescue StandardError => e
      %x(echo "#{e}" >> #{Rails.root}/log/dde_load_npids_errors.log)
    end
    i += 1
  end

  def create_table
    ActiveRecord::Base.connection.execute <<EOF
      CREATE TABLE npids (
      id serial PRIMARY KEY NOT NULL UNIQUE,
      couchdb_ref bigint NOT NULL,
      npid varchar(255) NOT NULL,
      version_number varchar(255) NOT NULL,
      assigned boolean NOT NULL DEFAULT 'f',
      created_at timestamp NOT NULL,
      updated_at timestamp NOT NULL);
EOF
  end