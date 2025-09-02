
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


create or alter procedure bronze.load_bronze  as  
begin 
    declare @start_time DATETIME , @end_time datetime ,@batch_start_time DATETIME ,@batch_end_time DATETIME ; 
	begin try 
	    set @batch_start_time =GETDATE() ;
		print'---------------------------------------' ;
		print'loading bronze layer'  ;
		print'---------------------------------------' ;

 

		---data insertion for the CRM files 
		print'---------------------------------------';
		print'loading crm tables' ;
		print'---------------------------------------';

		---data insertion for crm_cust_info 
		print'-------------------------------------------';
		print'truncate crm_cust_info'
		print'-------------------------------------------';

		truncate table bronze.crm_cust_info ;  --make the table empty 
		---data insertion 
		print'---------------------------------------';
		print'load crm_prd_info table '
		print'---------------------------------------';
		
		set @start_time=GETDATE() ; ---define the moment of loading start  

		bulk insert bronze.crm_cust_info 
		from 'C:\Users\user\Desktop\Projets\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
		with (
		   firstrow = 2 , 
		   fieldterminator = ',' , 
		   tablock 
		) ;

		set @end_time =GETDATE(); --- define the momemt of end loading ;
		print'>>> load duration : '  + CAST(DATEDIFF(SECOND,@start_time,@end_time)as nvarchar)+'secondes'; --- calculate the difference ;
		print'>>>----------------------------------------------';



	

		--verification 
		select * from bronze.crm_cust_info ;  ---verfication of distrubtion of columns and the quality of data insertion 

		select count(*) from bronze.crm_cust_info ;  ---verification of rows number compare with the source file (csv)   




		--- data insertion for crm_cust_prd_info 
		print'-------------------------------------------';
		print'Truncate crm_cust_info' ;
		print'-------------------------------------------';

		
		truncate table  bronze.crm_prd_info ; --- make the table empty (condition to avoid any type of errors or mistake) 

		---data insertion 
		print'-------------------------------------------';
		print'load crm_cust_prd_info table' ;
		print'-------------------------------------------';

		set @start_time =GETDATE(); 
		bulk insert bronze.crm_prd_info  
		from 'C:\Users\user\Desktop\Projets\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
		with ( 
		   firstrow = 2 ,
		   fieldterminator = ',',
		   tablock 
		) ;  

		set @end_time =GETDATE(); --- define the momemt of end loading
		print'>>> load duration : ' + cast(datediff(second ,@start_time,@end_time)as nvarchar)+'secondes'; --- calculate the difference 
		print'>>>----------------------------------------------'


		select * from bronze.crm_prd_info ; ---verfication of distrubtion of columns and the quality of data insertion 
		select count(*) from bronze.crm_prd_info ;  ---verification of rows number compare with the source file (csv)   



		--- data insertion for crm_sales_details 
		print'-------------------------------------------';
		print'truncate crm_sales_details';
		print'-------------------------------------------';

		truncate table bronze.crm_sales_details ; --make the table empty to avoid any errors or mistake can happen 

		---data insertion 
		print'-------------------------------------------';
		print'load crm_sales_details table' ;
		print'-------------------------------------------';

		set @start_time = GETDATE(); 
		bulk insert bronze.crm_sales_details 
		from 'C:\Users\user\Desktop\Projets\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' 

		with ( 
		  firstrow = 2 , 
		  fieldterminator =',' , 
		  tablock  
		  ) ;  
		set @end_time =GETDATE(); --- define the momemt of end loading
		print'>>> load duration : ' +  cast(datediff(second ,@start_time,@end_time)as nvarchar)+'secondes'; --- calculate the difference 
		print'>>>----------------------------------------------'


		
		select *from bronze.crm_sales_details ;  ---verfication of distrubtion of columns and the quality of data insertion 
		select count(*) from bronze.crm_sales_details ; ---verification of rows number compare with the source file (csv)    

		  



		--- data insertion for the ERP source files  
		print'---------------------------------------';
		print'loading erp tables' ;
		print'---------------------------------------';
	
	
	

		print'-------------------------------------------';
		print'truncate erp_CUST_AZ12';
		print'-------------------------------------------';

		truncate table bronze.erp_CUST_AZ12 ; -- make the table empty to avoid any type of errors or mistakes can happen 

		--data insertion 
		print'-------------------------------------------';
		print'load erp_CUST_AZ12 table' ;
		print'-------------------------------------------';

		set @start_time=GETDATE();
		bulk insert bronze.erp_CUST_AZ12 
		from  'C:\Users\user\Desktop\Projets\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
		with ( 
		   firstrow = 2 , 
		   fieldterminator = ',' , 
		   tablock 
		   ) ;  
		   
       	set @end_time =GETDATE(); --- define the momemt of end loading
		print'>>> load duration : ' + cast(datediff(second ,@start_time,@end_time)as nvarchar)+'secondes'; --- calculate the difference 
		print'>>>----------------------------------------------' ;

		select * from bronze.erp_CUST_AZ12 ; -- verification for the  distrubtion of rows and columns and data quality insertion  
		select count(*) from bronze.erp_CUST_AZ12 ; --- verification of rows number compare with the source file



		---data insertion for ERP loc_A101 file   
		print'-------------------------------------------';
		print'truncate ERP loc_A101' ; 
		print'-------------------------------------------';


		truncate table bronze.erp_LOC_A101 ;  -- make the table empty to avoid any type of errors or mistakes can happen 

		---data insertion 
		print'-------------------------------------------';
		print'load ERP loc_A101' ; 
		print'-------------------------------------------';
	    
		set @start_time =GETDATE() ; 
		bulk insert bronze.erp_LOC_A101 
		from 'C:\Users\user\Desktop\Projets\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
		with (
		   firstrow = 2 ,
		   fieldterminator =',', 
		   tablock 
		   );   
		set @end_time =GETDATE(); --- define the momemt of end loading
		print'>>> load duration : '  + cast(datediff(second ,@start_time,@end_time)as nvarchar)+'secondes'; --- calculate the difference 
		print'>>>----------------------------------------------'


		select * from bronze.erp_LOC_A101 ; -- verification for the  distrubtion of rows and columns and data quality insertion 
		select count(*) from bronze.erp_LOC_A101 ;  --- verification of rows number compare with the source file

   
		--data insertion for ERP PX_CAT_G1_v1  
		print'-------------------------------------------';
		print'truncate ERP PX_CAT_G1_v1' ;
		print'-------------------------------------------';

		truncate table bronze.erp_PX_CAT_G1v1 ; -- make the table empty to avoid any type of errors or mistakes can happen 

		---data insertion 
		print'-------------------------------------------';
		print'load erp_PX_CAT_G1_v1 table' ;
		print'-------------------------------------------';


		set @start_time =GETDATE() ;
		bulk insert bronze.erp_PX_CAT_G1v1 
		from 'C:\Users\user\Desktop\Projets\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
		with (
			firstrow= 2 , 
			fieldterminator =',', 
			tablock 
			);  
		set @end_time =GETDATE(); --- define the momemt of end loading
		print'>>> load duration : ' + cast(datediff(second ,@start_time,@end_time)as nvarchar)+'secondes'; --- calculate the difference 
		print'>>>----------------------------------------------'

		select * from bronze.erp_PX_CAT_G1v1 ;  -- verification for the  distrubtion of rows and columns and data quality insertion 
		select count(*) from bronze.erp_PX_CAT_G1v1 ;  --- verification of rows number compare with the source file

		set @batch_end_time=GETDATE()  ;
		print'======================================================' ;
		print'Loading Bronze layer is completed' ;
		print'Total load duration :' + Cast(DATEDIFF(Second,@batch_start_time,@batch_end_time) as nvarchar) +'secondes' ;
		print'======================================================' ;



	end try 
	begin catch 
	    print'======================================================'
		print'Error Occured during loading bronze layer'
		print'Error Message'+ERROR_MESSAGE() ; 
		print'Error Message' +CAST (ERROR_NUMBER() as nvarchar);
		print'Error Message' +CAST (ERROR_state() as nvarchar);
		print'======================================================'

	end catch 
end 





    











