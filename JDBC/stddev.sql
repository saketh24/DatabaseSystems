create procedure calcstd(out stddev float(24))
	language sql
	begin
	declare sqlstate char(5) default '00000';
	declare sum1 bigint;
	declare square_sum bigint;
	declare sal bigint;
	declare std float(24);
	declare count1 bigint;
	declare mean float(24);
	declare c cursor for select salary from employee;
	set sum1 =0;
	set square_sum = 0;
	set count1 =0;
	set std = 0;
	open c;
	fetch from c into sal;
	while (sqlstate = '00000') do
		set count1 = count1 + 1;
		set sum1 = sum1 + sal;
		set square_sum = square_sum + (sal*sal);
		fetch from c into sal;
	end while;
	close c;
	set mean = sum1/count1;
	set std = (square_sum/count1) - (mean*mean);
	set std = sqrt(std);
	set stddev = std;
	end@