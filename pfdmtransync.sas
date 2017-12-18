/* 03/16/2017 UPDATE:  ESL  Use pass thru SQL */

%macro PfdmtranSync ;

%LET backdate = %SYSFUNC(putn(%SYSFUNC(today())-180,yymmddn8.)) ;

    proc sql;
      connect to odbc as mydb (datasrc=avalon user=rdsuser password=!4ne12N0) ;
      create table updates1 as select * from connection to mydb(
              select v.vehicleid as vin, l.LocationCode, s.*
                    from VehicleSales s
                    left join Vehicles v on s.VehicleKey = v.VehicleKey
                    left join Locations l on s.LocationKey = l.LocationKey
			where date(s.modifydate)>=date(now() AT TIME ZONE 'America/New_York')-60 /*20161103 update: limit to past 2 months*/
			;
                    )  ;
               disconnect from mydb;
      quit ;

/*20150408 update: change to explicit pass through*/
      /*    data  pfdmtran60 ; set bhagmer.pfdmtran60 ; run;*/
     proc sql;
          connect to odbc as mydb (datasrc=&merdb user=rdsuser password=!4ne12N0) ;
          create table pfdmtran60 as select * from connection to mydb
          ( ;
            select *
            from  bhagmer.pfdmtran60
			where scode='SF' /*20161212 update: https://www.pivotaltracker.com/n/projects/1054818/stories/135530669*/
			;
          ) ;
          disconnect from mydb ;
     quit ;
/*20150408 update end*/


     proc sql noprint ;
          create table updates as
          select a.VehicleSaleKey, b.*
          from updates1 a,
               pfdmtran60 b
          where a.VIN = b.sser17 and a.LocationCode = b.sauci and a.sblu = b.sblu ;
     quit ;

     data updates2 ;
          set updates ;
          if sslepr = 0 then sslepr = . ;
          if sbuyfe = 0 then sbuyfe = . ;
          auctiondate= input(put(sdtesl,Z8.),yymmdd8.); /*20160209 update: add auctiondate*/
     run ;

     proc sort data=updates2 ;
     /*07/30/2014 update: for same set of sser17, sauci, sblu, descending sdtesl to keep latest record, instead of oldest one*/
          by VehicleSaleKey descending sdtesl ;
     run ;

     proc sort data=updates2 nodupkey ;
          by VehicleSaleKey ;
     run ;

/*20141112 update: change to expicit pull*/

/*     data VehicleSales ;*/
/*          set avalon.VehicleSales ;*/
/*     run ;*/

/*     proc sql;*/
/*          connect to odbc as mydb (datasrc=&dbref user=rdsuser password=!4ne12N0) ;*/
/*          create table VehicleSales as select * from connection to mydb*/
/*          ( ;*/
/*            select **/
/*            from  VehicleSales;*/
/*          ) ;*/
/*          disconnect from mydb ;*/
/*     quit ;*/
/*20141112 update end*/

/*     proc sort data=VehicleSales nodupkey ;*/
/*          by VehicleSaleKey ;*/
/*     run ;*/

	    proc sort data=updates1 nodupkey ;
          by VehicleSaleKey ;
     run ;

/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,Buyer5Mil,buid) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,AuctionSaleYear,ssleyr) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,AuctionSaleNum,ssale_) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,AuctionLaneNum,slane_) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,AuctionRunNum,srun_) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,VehicleSalePrice,sslepr) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,VehicleSaleFee,sbuyfe) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,HowPaid,shwpd) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,DepositDate,sdtedp) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,DatePaid,sdtepd) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,DateTitleRcvd,sdtefo) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,DateTitleSent,sdtets) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,sflragcy,sflragcy) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,swo,swo_) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,ArbFlag,dmarb) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,ArbFlag,dmabdisp) ;*/
/*     %AuditLog(VehicleSales,updates2,audit.VehicleSales,VehicleSaleKey,PsiFlag,dmpsiflg) ;*/

     proc compare base=updates1/*VehicleSales*/ compare=updates2 nosummary out=compdata noprint outnoequal outall ;
          by VehicleSaleKey ;
          var Buyer5Mil 
                auctiondate /*20160209 update: add auctiondate*/
                AuctionSaleYear AuctionSaleNum AuctionLaneNum AuctionRunNum
              VehicleSalePrice VehicleSaleFee HowPaid DepositDate DatePaid sflrty
              DateTitleRcvd DateTitleSent dmagencyid sflragcy swo ArbFlag PsiFlag dmabdisp ;
          with buid
                auctiondate /*20160209 update: add auctiondate*/
                ssleyr ssale_ slane_ srun_ sslepr sbuyfe shwpd sdtedp sdtepd
               sflrty sdtefo sdtets dmagencyid sflragcy swo_ dmarb dmpsiflg dmabdisp ;
     run ;

     proc sql noprint ;
          create table syncvsales as
          select 
			/***/ /*20161103 update: select specific values*/
			vehiclesalekey,sauci,ssleyr,sdtesl,sdtedp,sdtepd,shwpd,sdtefo,dmarb,ssale_,slane_,srun_,swo_,sblu,sser17,dmmodelyr,
			dmmake,dmmodel,dmbody,dmjdcat,dmjdsubcat,sannou,smiles,sslepr,sfloor,mstcym,ssellr,suid,sbuyer,dmbuyrepid,buid,dmcat,sbuyfe,
			dmcertfee,dmtransprt,dmabdisp,stitle,smsoti,sbuyne,sdftfe,sadj2,sbuydisc,dmovebfee,sprimcd,dmlightr,sflragcy,dmsimbuyfe,
			sdtets,dmpsiflg,dmhighbid,dmhightbid,sflrty,dmagencyid,scond,auctiondate
		 from updates2
          where VehicleSaleKey in (select distinct VehicleSaleKey from compdata) ;
     quit ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute( TRUNCATE TABLE temp.syncvsales ; ) by mydb ;
          disconnect from mydb ;
     quit ;

     proc append base=temp.syncvsales data=syncvsales ;
     run ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute(
                     UPDATE VehicleSales a
                     SET Buyer5Mil = b.buid,
                            auctiondate=b.auctiondate, /*20160209 update: update auctiondate*/
                         AuctionSaleYear = b.ssleyr, AuctionSaleNum = b.ssale_, AuctionLaneNum = b.slane_,
                         AuctionRunNum = b.srun_, VehicleSalePrice = coalesce(b.sslepr,a.VehicleSalePrice),
                         VehicleSaleFee = coalesce(b.sbuyfe,a.VehicleSaleFee), HowPaid = b.shwpd,
                         DepositDate = b.sdtedp, DatePaid = b.sdtepd, sflrty = b.sflrty,
                         DateTitleRcvd = b.sdtefo, DateTitleSent = b.sdtets, dmagencyid = b.dmagencyid,
                         PfdmtranCheck = 'Y', sflragcy = b.sflragcy, swo = b.swo_,
                         ArbFlag = b.dmarb, PsiFlag = b.dmpsiflg, dmabdisp = b.dmabdisp
                     FROM temp.syncvsales b
                     WHERE a.VehicleSaleKey = b.VehicleSaleKey ;

                  ) by mydb ;

          disconnect from mydb ;
     quit ;
/*20151105 update: OVT conversion logic*/
/*     options symbolgen;*/
/*      proc sql;*/
/*          connect to odbc as mydb (datasrc=&dbref user=rdsuser password=!4ne12N0) ;*/
/*          create table Locations as select * from connection to mydb*/
/*          ( ;*/
/*            select locationcode*/
/*            from  Locations where g2g='Y';*/
/*          ) ;*/
/*          disconnect from mydb ;*/
/*          select catt("'", locationcode, "'") into :locs separated by ', ' from locations;*/
/*     quit ;*/
/*20160122 update: retreive conversion date g2g*/
     options symbolgen;
      proc sql;
          connect to odbc as mydb (datasrc=&dbref user=rdsuser password=!4ne12N0) ;
          create table Locations as select * from connection to mydb
          ( ;
            select locationcode, g2g
            from  Locations where g2g > '2015-11-08';
          ) ;
          disconnect from mydb ;
          select catt("'", locationcode, "'") into :locs separated by ', ' from locations;
     quit ;
/*20160122 update end*/

		proc sql;
         connect to db2 as mydb (datasrc=BHAG user=Dealshield password=Hello123) ;
            create table prices1 as select * from connection to mydb
         (     SELECT AJSLEYR as Sale_Year, AJSALE# as Sale_No, AJLANE# as Lane_No, AJRUN#  as Run_No,
                 AJREC# as Rec_No, AJNOTE as Note_New, AJAMT as BBGPrice_New, AJAUCI as Location, AJDTEADD as BBGDate_New
          from masterf.PFMSTSADJ
          where AJCODE in ('DEALB', 'DSHD', 'DEALS'
							,'DSMPS','DSNPA','DSNPB','DSNPS','DEALA')  /*20160323 update: additional subclass codes with DS fees*/
				and AJDTEADD>=&backdate ;/*20161103 update: limit to 6 months*/
         );
        disconnect from mydb;
        quit;

%let nfail=0;
%OVT_fees:
     proc sql;
        connect to oracle as mydb (path=OVT_OLTP user=OVT_DS password=povtds1022) ;
        create table prices1a as select * from connection to mydb
        (
            SELECT 
          c.INVC_LINE_AMT as  BBGPrice_New /*20160609 update: c.ADJ_AMT AS BBGPrice_New  */
          ,cast(SUBSTR(a.AUCTION_CD,1,4) as char(4)) AS Location 
/*          ,d.LINE_ITEM_NUM AS AJCODE */
/*          ,SUBSTR(c.LINE_ITEM_DESC,1,30)  AS AJDESC*/
          ,c.INVC_DT_KEY AS BBGDate_New 
          ,a.LANE_NUM AS Lane_No
          ,c.INVC_LINE_KEY AS Rec_No 
          ,a.RUN_NUM AS Run_No
          ,a.SALE_NUM AS Sale_No 
          ,a.SALE_YEAR AS Sale_Year
          ,cast('' as char(40)) as Note_New
    FROM  OVTAPP.FACT_REGISTRATION a
          LEFT OUTER JOIN OVTAPP.FACT_INVOICE b ON a.REG_KEY = b.REG_KEY 
          LEFT OUTER JOIN OVTAPP.FACT_INVOICE_LINE c ON b.INVC_KEY = c.INVC_KEY
          LEFT OUTER JOIN OVTAPP.DIM_INVOICE_LINE_ITEM d ON c.INVC_LINE_ITEM_KEY = d.INVC_LINE_ITEM_KEY
    WHERE c.INVC_DT_KEY >= 20151106
		  AND c.INVC_DT_KEY >=&backdate /*20161103 update: limit to past 6 months*/
          AND a.NET_TXN_FLG = 1
          AND b.INVC_TYPE = 'BUYER'
/*          AND c.LINE_TYPE = 'ADJUST'*/ /*20160609 update:*/
          AND a.AUCTION_CD in (&locs) and  d.LINE_ITEM_NUM  in ('14D250M','14D500M','21D250M','21D500M','21N60D250M','21N60D500M',
                                                                '60D250M','7D250M','7D500M'
              ,'14D360M','14D360M','21D360M','21D360M','21N60D360M','21N60D360M','60D360M','7D360M','7D360M' /*20160122 update: add new line items*/
                                                                )
            );
            disconnect from mydb;
     quit;

	%if  &SQLXRC ne 0 and &nfail < 5 %then
         %do ;
             %let nfail = %eval(&nfail+1) ;      
             %goto OVT_fees ;
         %end ;
	%if &nfail=5 %then
		%do;
			%EmailNotify(Daily Update Error,PfdmtranSync OVT Fees Pull connection) ;
			proc sql; create table prices1a like prices1; quit;
		%end;

    /*20160122 update: pull G2G converted locations' adjustments by conversion date*/
         proc sql;
         create table prices1aa as 
             select m.*
             from prices1a as m left join Locations l
                on m.location=l.locationcode 
                where l.g2g ne . and  input(put(m.BBGDate_New,z8.), yymmdd8.)>= l.g2g  ;
         quit;

     proc append base = prices1 data = prices1aa; run;
     /*20160122 update end*/
/*     20151105 update end*/

/*Update 20141023: add BSC data*/
%let nfail=0;
%BELA_fees: 
proc sql;
connect to db2 as mydb (datasrc=BELA user=Dealshield password=Hello123) ;
create table prices1_BSC as select * from connection to mydb
(
    select DISTINCT
           AJSLEYR as Sale_year,
           AJSALE# as Sale_No,
           AJLANE# as Lane_No,
           AJRUN#  as Run_No,
           AJREC# as Rec_No,
           AJNOTE as Note_New,
           AJAMT as BBGPrice_New,
           AJDTEADD as BBGDate_New, 'BELA' as location
    from MACSF.pfsadjdtl 
    where AJCODE in ('DEALB', 'DSHD', 'DEALS'
					,'DSMPS','DSNPA','DSNPB','DSNPS','DEALA')  /*20160323 update: additional subclass codes with DS fees*/
									and AJDTEADD>=&backdate; /*20161103 update: limit to 6 months*/
);
disconnect from mydb;
quit;

	%if  &SQLXRC ne 0 and &nfail < 5 %then
         %do ;
             %let nfail = %eval(&nfail+1) ;      
             %goto BELA_fees ;
         %end ;
	%if &nfail=5 %then
		%do;
			%EmailNotify(Daily Update Error,PfdmtranSync BELA Fees Pull connection) ;
			proc sql; create table prices1_BSC like prices1a; quit;
		%end;

%let nfail=0;
%TALA_fees: 

  proc sql;
connect to db2 as mydb (datasrc=TALA user=Dealshield password=Hello123) ;
create table prices1_BSC2 as select * from connection to mydb
(
    select DISTINCT
           AJSLEYR as Sale_year,
           AJSALE# as Sale_No,
           AJLANE# as Lane_No,
           AJRUN#  as Run_No,
           AJREC# as Rec_No,
           AJNOTE as Note_New,
           AJAMT as BBGPrice_New,
           AJDTEADD as BBGDate_New, 'TALA' as location
    from MACSF.pfsadjdtl 
    where AJCODE in ('DEALB', 'DSHD', 'DEALS'
					,'DSMPS','DSNPA','DSNPB','DSNPS','DEALA')  /*20160323 update: additional subclass codes with DS fees*/
		  and AJDTEADD>=&backdate; /*20161103 update: limit to 6 months*/

);
disconnect from mydb;
quit;

	%if  &SQLXRC ne 0 and &nfail < 5 %then
         %do ;
             %let nfail = %eval(&nfail+1) ;      
             %goto TALA_fees ;
         %end ;
	%if &nfail=5 %then
		%do;
			%EmailNotify(Daily Update Error,PfdmtranSync TALA Fees Pull connection) ;
			proc sql; create table prices1_BSC2 like prices1a; quit;
		%end;


data prices1_BSC; set prices1_BSC/*2*/ prices1_BSC2; run;

proc sql;
insert into prices1
            (
                Sale_year,Sale_No,Lane_No,Run_No,Rec_No,Note_New,BBGPrice_New,
                    Location,
                BBGDate_New
            )
select 
                Sale_year,Sale_No,Lane_No,Run_No,Rec_No,Note_New,BBGPrice_New,
                   Location,
                BBGDate_New
from prices1_BSC
;
quit;
/*Update 20141023 end*/

     proc means data=prices1 nway noprint ;
          class Sale_Year Location Sale_No Lane_No Run_No ;
          id Note_New BBGDate_New ;
          var BBGPrice_New ;
          output out=prices2(drop=_type_ _freq_) sum=PriceMH ;
     run ;

     proc sort data=prices2 nodupkey ;
          by sale_year Location Sale_No Lane_No Run_No ;
     run ;

     proc sql;
      connect to odbc as mydb (datasrc=avalon user=rdsuser password=!4ne12N0) ;
      create table dealshields as select * from connection to mydb
          (
          select a.DealShieldKey, a.PriceDS, a.PriceMH, a.OfferKey, b.LocationKey, d.PartnerKey, e.VRA, c.swo,
                 b.VehicleSaleKey, c.pfdmtrancheck, c.AuctionSaleYear as sale_year, c.AuctionSaleNum as sale_no,
                 c.AuctionLaneNum as lane_no, c.AuctionRunNum as run_no, d.LocationCode as location
				 ,a.consignment_id, f.vehicleid, c.sblu /*20161103 update: extract new fields*/
          from DealShields a
               left join Offers b on a.OfferKey=b.OfferKey
               left join VehicleSales c on b.VehicleSaleKey=c.VehicleSaleKey
               left join Locations d on b.LocationKey=d.LocationKey
               left join Returns e on a.DealShieldKey = e.DealShieldKey
			   left join vehicles f on c.vehiclekey = f.vehiclekey; /*20161103 update: new join*/
         )  ;
               disconnect from mydb;
      quit ;

     proc sort data=Dealshields ;
          by sale_year Location Sale_No Lane_No Run_No descending VRA descending pfdmtrancheck ;
          where location not in ('FTB','DLRM') and PartnerKey in (1,3,5) and not missing(swo) ; /*20171013 update: add auctionEdge*/
     run ;

     proc sort data=Dealshields nodupkey out=Dealshields2; /*20171013 update*/
	 where PartnerKey in (1,3);
          by sale_year Location Sale_No Lane_No Run_No ;
     run ;

     proc sql noprint ;
          create table dsupdates as
          select a.DealShieldKey, b.*
		  , c.consignment_id /*20161103 update: add new field*/
          from Dealshields2 a  /*20171013 update*/
			   left join prices2 b
		 	   	on a.Location = b.location and a.sale_year=b.sale_year and a.sale_no=b.sale_no and a.lane_no=b.lane_no and a.run_no=b.run_no 
		  	   left join  pfdmtran60 c /*20161103 update: add new join*/
          		on a.vehicleid = c.sser17 and a.location = c.sauci and a.sblu = c.sblu 
          ;
     quit ;

/*20170903 update: get external fees*/
	proc sql;
          connect to odbc as mydb (datasrc=avalon user=rdsuser password=!4ne12N0) ;
          create table externalfees as select * from connection to mydb
          ( ;
            select *
            from externalfees
             where 
			 	processed = 'N' and date(insertdate)>= to_date(to_char(&backdate,'00000000'), 'yyyymmdd')
            ;
          ) ;
          disconnect from mydb ;
     quit ;

	 proc sql;
	 create table external_ds as 
	 select e.externalfeeskey , d.dealshieldkey, d.vehiclesalekey, e.vin as vehicleid, e.sblu, e.auctioncode as location, e.dealshieldfee as pricemh,
			e.buyeradjustments, . as consignment_id, datepart(insertdate) as adjustdate
	 from externalfees e left join dealshields d on e.sblu=d.sblu and e.vin=d.vehicleid and e.auctioncode=d.location;
	 quit;

	proc sort data= external_ds out=external_ds_upd nodupkey;
	 by DealShieldKey;
	 where DealShieldKey>0 and pricemh>0; 
	run;

	data dsupdates;
		set dsupdates external_ds_upd;
	run;

	proc sort data=dsupdates; by DealShieldKey pricemh; run;
/*20170903 update end*/

     proc sort data=Dealshields nodupkey ;
          by DealShieldKey ;
     run ;

     proc sort data=dsupdates nodupkey ;
          by DealShieldKey ;
     run ;

/*     %AuditLog(Dealshields,dsupdates,audit.Dealshields,DealShieldKey,PriceMH,PriceMH) ;*/

     proc compare base=Dealshields compare=dsupdates nosummary out=compdata noprint outnoequal ;
          by DealShieldKey ;
          var PriceMH  consignment_id; /*20161103 update: add new variable*/
          with PriceMH consignment_id; /*20161103 update: add new variable*/
     run ;

/*20161103 update: add missing factor*/
data compdata;
set compdata;
     if consignment_id = .E then consignment_id=.;
run;
/*20161103 update end */

     proc sql noprint ;
          create table syncds as
          select DealShieldKey, PriceMH
		   , consignment_id /*20161103 update: add new variable*/
          from dsupdates
          where DealShieldKey in (select distinct DealShieldKey from compdata /* where pricemh ^= . or consignment_id ^= .*/) ; /*20171013 update*/
     quit ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute( TRUNCATE TABLE temp.syncds ; ) by mydb ;
          disconnect from mydb ;
     quit ;

     proc append base=temp.syncds data=syncds ;
     run ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute(
                     UPDATE DealShields a
                     SET PriceMH = b.PriceMH
					 , consignment_id=b.consignment_id /*20161103 update: add new variable*/
                     FROM temp.syncds b
                     WHERE a.DealShieldKey = b.DealShieldKey ;
                  ) by mydb ;

          disconnect from mydb ;
     quit ;

/******************************************************
     proc sql noprint ;
          create table arbdeals as
          select a.DealShieldKey, -1 as StatusDS
          from avalon.DealShields a, avalon.Offers b,
               (select VehicleSaleKey from avalon.VehicleSales
                where ArbFlag = 'Y') c
          where a.OfferKey=b.OfferKey and b.VehicleSaleKey=c.VehicleSaleKey ;
     quit ;

     data DealShields ;
          set avalon.DealShields ;
     run ;

     proc sort data=DealShields ;
          by DealShieldKey ;
     run ;

     proc sort data=arbdeals ;
          by DealShieldKey ;
     run ;

     %AuditLog(Dealshields,arbdeals,audit.Dealshields,DealShieldKey,StatusDS,StatusDS) ;

     data temp.updatestatusds ;
          set arbdeals(keep=DealShieldKey) ;
     run ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute(
                     UPDATE DealShields a
                     SET StatusDS = -1
                     FROM temp.updatestatusds b
                     WHERE a.DealShieldKey = b.DealShieldKey ;

                  ) by mydb ;

          disconnect from mydb ;
     quit ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute( DROP TABLE temp.updatestatusds ; ) by mydb ;
          disconnect from mydb ;
     quit ;

*****************************************************/
     proc sql;
      connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
      create table diffvals1 as select * from connection to mydb
          (
          select a.DealShieldKey, a.AssuredValue, a.AssuredFees, c.VehicleSalePrice,
                 c.VehicleSaleFee
          from DealShields a, offers b, VehicleSales c
          where a.OfferKey=b.OfferKey and b.VehicleSaleKey=c.VehicleSaleKey and
                (c.VehicleSalePrice > 0 or c.VehicleSaleFee > 0) and
                (a.AssuredValue <> c.VehicleSalePrice or a.AssuredFees <> c.VehicleSaleFee ) ;
         )  ;
               disconnect from mydb;
      quit ;

      data diffvals;
          set diffvals1;
          AssuredTotal = sum(VehicleSalePrice,VehicleSaleFee) ;
      run;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute( TRUNCATE TABLE temp.syncdiffvals ; ) by mydb ;
          disconnect from mydb ;
     quit ;

     proc append base=temp.syncdiffvals data=diffvals ;
     run ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute(
                     UPDATE DealShields a
                     SET AssuredValue = b.VehicleSalePrice,
                         AssuredFees = b.VehicleSaleFee,
                         AssuredTotal = b.AssuredTotal
                     FROM temp.syncdiffvals b
                     WHERE a.DealShieldKey = b.DealShieldKey ;
                  ) by mydb ;

          disconnect from mydb ;
     quit ;

     /* Update Fees & Adjustments Table */


/*20141112 update: explicit proc sql, pull past 3 months*/
     %let pulldate=%sysfunc(putn(%eval(%sysfunc(today())-100),yymmddn8.));      
    %put &pulldate;
     /*     %let qyrm1 = %eval(&qyear - 1) ;*/
	proc sql;
		connect to db2 as mydb (datasrc=BHAG user=Dealshield password=Hello123) ;
		create table feesadj as select * from connection to mydb
		(     SELECT AJSLEYR as SaleYear, AJSALE# as SaleNum, AJLANE# as LaneNum, AJRUN#  as RunNum,
		     AJREC# as RecNum, cast(AJCODE as char(24)) as  AJCODE, AJDESC, AJNOTE, AJAMT, AJAUCI as Location, AJDTEADD
		from masterf.PFMSTSADJ
		/*            where AJSLEYR in (&qyrm1,&qyear) ;*/
		where AJDTEADD>&pulldate and ajamt <> 0 ;
		);
		disconnect from mydb;
	quit;
/* 20141112 update end*/


/*20170516 update: resolve data overlapping issue for feesadj between bhag and ovt, by filtering out g2g in bhag, and keeping only g2g in ovt*/
	proc sql;
		connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
		create table LocALL as select * from connection to mydb
		( ;
		select locationcode, g2g
		from locations
		) ;
		disconnect from mydb ;
	quit ;
	proc sql;
		create table feesadj as
		select a.*
		from feesadj a
		inner join LocALL b
			on a.location=b.locationcode 
		where a.AJDTEADD < year(b.g2g)*10000+month(b.g2g)*100+day(b.g2g)
			OR b.g2g is null
		;
	quit;
/*20170516 update ends*/



/*20151105 update: add G2G converted logic from OVT*/
/*libname ovt oracle user = OVT_DS password = povtds1022  path =  OVT_OLTP schema =  OVTAPPADM  ;*/
%let nfail=0;
%feesadjOVT: 

             proc sql;
                 connect to oracle as mydb (path=OVT_OLTP user=OVT_DS password=povtds1022) ;
                    create table feesadjOVT as select * from connection to mydb
                    (
                        SELECT 
            /*a.REG_KEY 0*/
				  c.INVC_LINE_AMT AS AJAMT /* c.ADJ_AMT AS AJAMT  */ /*20160609 update: amount is INVC_LINE_AMT*/
				   ,d.CUR_INT_OPER_AREA  /*20160609 update: include new field*/
                  ,SUBSTR(a.AUCTION_CD,1,4) AS Location 
                  ,d.LINE_ITEM_NUM AS AJCODE 
                  ,SUBSTR(c.LINE_ITEM_DESC,1,30)  AS AJDESC
                  ,c.INVC_DT_KEY AS AJDTEADD 
                  ,a.LANE_NUM AS LaneNum
                  ,c.INVC_LINE_KEY AS RecNum 
                  ,a.RUN_NUM AS RunNum
                  ,a.SALE_NUM AS SaleNum 
                  ,a.SALE_YEAR AS SaleYear
              FROM  OVTAPP.FACT_REGISTRATION a
                  LEFT OUTER JOIN OVTAPP.FACT_INVOICE b ON a.REG_KEY = b.REG_KEY 
                  LEFT OUTER JOIN OVTAPP.FACT_INVOICE_LINE c ON b.INVC_KEY = c.INVC_KEY
                  LEFT OUTER JOIN OVTAPP.DIM_INVOICE_LINE_ITEM d ON c.INVC_LINE_ITEM_KEY = d.INVC_LINE_ITEM_KEY
              WHERE c.INVC_DT_KEY >= 20151106
                  AND c.INVC_DT_KEY >&pulldate
				  AND c.INVC_LINE_AMT <> 0 /*and ADJ_AMT <> 0*/ /*20160609 update*/
                  AND a.NET_TXN_FLG = 1
                  AND b.INVC_TYPE = 'BUYER'
/*                AND c.LINE_TYPE = 'ADJUST'*/ /*20160609 update: not applicable if all adjustments are needed*/
                  AND a.AUCTION_CD in (&locs)   
			  	  AND c.LINE_ITEM_DESC <> 'BUY FEE' /*20160609 update: exclude buy fees*/ 
                    );
                    disconnect from mydb;
              quit;

	%if  &SQLXRC ne 0 and &nfail < 5 %then
         %do ;
             %let nfail = %eval(&nfail+1) ;      
             %goto feesadjOVT ;
         %end ;
	%if &nfail=5 %then
		%do;
			%EmailNotify(Daily Update Error,PfdmtranSync OVT Fees Pull connection) ;
			proc sql; create table feesadjOVT like feesadj; quit;
		%end;

/*20160308 update: patch missing line item number for FloorPlanFee*/
        data feesadjOVT (drop=CUR_INT_OPER_AREA);
            set feesadjOVT;
			where CUR_INT_OPER_AREA ne 'VEHICLE PRICE';
            if AJDESC='Floorplan Fee' then AJCODE='3PFP';
        run;
/*20160308 update end*/


/*20170516 update: resolve data overlapping issue for feesadj between bhag and ovt, by filtering out g2g in bhag, and keeping only g2g in ovt*/
	proc sql;
		connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
		create table LocG2G as select * from connection to mydb
		( ;
		select locationcode, g2g
		from  Locations where g2g > '2015-11-08';
		) ;
		disconnect from mydb ;
	quit ;
	proc sql;
		create table feesadjOVT as
		select a.*
		from feesadjOVT a
		inner join LocG2G b
			on a.location=b.locationcode 
		where a.AJDTEADD >= year(b.g2g)*10000+month(b.g2g)*100+day(b.g2g)
		;
	quit;
/*20170516 update ends*/

        proc sql;
        insert into feesadj
                    (
                        SaleYear,SaleNum,LaneNum,RunNum,RecNum,AJCODE,AJDESC,AJAMT,
                            Location, AJDTEADD
                    )
        select 
                        SaleYear,SaleNum,LaneNum,RunNum,RecNum,AJCODE,AJDESC,AJAMT,
                            Location, AJDTEADD
        from feesadjOVT
        ;
        quit;
/*20151105 update end*/

    
/* 20141023 Update: add BSC data*/
%let nfail=0;
%feesadj_BSC: 

        proc sql;
        connect to db2 as mydb (datasrc=BELA user=Dealshield password=Hello123) ;
        create table feesadj_BSC as select * from connection to mydb
        (
            select AJSLEYR as SaleYear, 
                   AJSALE# as SaleNum, 
                   AJLANE# as LaneNum, 
                   AJRUN#  as RunNum,
                   AJREC# as RecNum, 
                   AJCODE, 
                   AJDESC, 
                   AJNOTE, 
                   AJAMT, 
                   AJDTEADD, 'BELA' as location
            from MACSF.pfsadjdtl 
/*            where AJSLEYR in (&qyrm1,&qyear) ;*/
           where  AJDTEADD>&pulldate and ajamt <> 0  /*20141112 update: change pull date and add ajamt <>0*/
        );
        disconnect from mydb;
        quit;
	%if  &SQLXRC ne 0 and &nfail < 5 %then
         %do ;
             %let nfail = %eval(&nfail+1) ;      
             %goto feesadj_BSC ;
         %end ;
	%if &nfail=5 %then
		%do;
			%EmailNotify(Daily Update Error,PfdmtranSync BELA Fees Pull connection) ;
			proc sql; create table feesadj_BSC like feesadj; quit;
		%end;

        proc sql;
        insert into feesadj
                    (
                        SaleYear,SaleNum,LaneNum,RunNum,RecNum,AJCODE,AJDESC,AJNOTE,AJAMT,
                            Location, AJDTEADD
                    )
        select 
                        SaleYear,SaleNum,LaneNum,RunNum,RecNum,AJCODE,AJDESC,AJNOTE,AJAMT,
                            Location, AJDTEADD
        from feesadj_BSC
        ;
        quit;

%let nfail=0;
%feesadj_BSC2: 
        proc sql;
        connect to db2 as mydb (datasrc=TALA user=Dealshield password=Hello123) ;
        create table feesadj_BSC2 as select * from connection to mydb
        (
            select AJSLEYR as SaleYear, 
                   AJSALE# as SaleNum, 
                   AJLANE# as LaneNum, 
                   AJRUN#  as RunNum,
                   AJREC# as RecNum, 
                   AJCODE, 
                   AJDESC, 
                   AJNOTE, 
                   AJAMT, 
                   AJDTEADD, 'TALA' as location
            from MACSF.pfsadjdtl 
/*            where AJSLEYR in (&qyrm1,&qyear) ;*/
           where  AJDTEADD>&pulldate and ajamt <> 0  /*20141112 update: change pull date and add ajamt <>0*/
        );
        disconnect from mydb;
        quit;
	%if  &SQLXRC ne 0 and &nfail < 5 %then
         %do ;
             %let nfail = %eval(&nfail+1) ;      
             %goto feesadj_BSC2 ;
         %end ;
	%if &nfail=5 %then
		%do;
			%EmailNotify(Daily Update Error,PfdmtranSync TALA Fees Pull connection) ;
			proc sql; create table feesadj_BSC2 like feesadj; quit;
		%end;

                proc sql;
        insert into feesadj
                    (
                        SaleYear,SaleNum,LaneNum,RunNum,RecNum,AJCODE,AJDESC,AJNOTE,AJAMT,
                            Location, AJDTEADD
                    )
        select 
                        SaleYear,SaleNum,LaneNum,RunNum,RecNum,AJCODE,AJDESC,AJNOTE,AJAMT,
                            Location, AJDTEADD
        from feesadj_BSC2
        ;
        quit;
/*20141023 update end*/

     proc sql noprint ;
          create table feesadj2 as
          select b.LocationKey, a.*
          from feesadj a left join avalon.Locations b on a.Location = b.LocationCode ;
     quit ;

     proc sql noprint ;
          create table feesadj3 as
          select b.VehicleSaleKey, a.AJCODE as AdjustCode, AJDESC as AdjustDesc, RecNum,
                 input(put(AJDTEADD,8.),yymmdd8.)  as AdjustDate, AJAMT as AdjustAmt, AJNOTE as AdjustNote
          from feesadj2 a, avalon.VehicleSales b
          where a.SaleYear = b.AuctionSaleYear and a.SaleNum = b.AuctionSaleNum and
                a.LaneNum = b.AuctionLaneNum and a.RunNum = b.AuctionRunNum and a.LocationKey = b.LocationKey ;
     quit ;


/*20170903 update:add AE codes*/
/*parse buyeradjustments from previously created dataset - exclude buyf*/

	 proc sql; 
		create table buyeradjustments as 
		 select buyeradjustments,vehiclesalekey, adjustdate,'' as adjustnote,'' as adjustdesc, countc(buyeradjustments, ':') as num_fees
		 from external_ds
		 where vehiclesalekey>0;
		 select max(num_fees) into: fees from buyeradjustments  ; 
	 quit;

	 proc sort data=buyeradjustments nodupkey; by vehiclesalekey; run;
	
	 data ext_fee (drop=i pos pos2 pos3 num_fees buyeradjustments);
	 set buyeradjustments;
	 do i=1 to &fees;
	 	 call scan(buyeradjustments,i+1,pos,len,':');
		 call scan(buyeradjustments,i+1,pos2,len2,';');
		  pos =pos-1;
		  pos2=pos2-1;
		  pos3=lag(pos2);
		  if i=1 then adjustcode=substr(buyeradjustments, 1, pos-i);
		  if i>1 then adjustcode=substr(buyeradjustments, pos3+1,pos-pos3-1 );
		  adjustamt=input(substr(buyeradjustments, pos+1,pos2-pos-1 ), 8.);
		  recnum=i;
		  drop len len2;
		  if adjustamt>0 and adjustcode ne 'BUYF' then output;
	 end;
	 run;

	 data feesadj3;
	 set feesadj3 ext_fee;
	 run;
	
/*20170903 update end*/

     proc sort data=feesadj3 out=codes(keep=AdjustCode AdjustDesc) nodupkey ;
          by AdjustCode ;
     run ;

     proc sql noprint ;
          create table NewCodes as
          select *
          from codes where AdjustCode not in (select AdjustCode from avalon.AdjustCodes) and adjustcode ne ''; /*2014112 update: add and adjustcode ne ''*/
     quit ;

     proc sql noprint ;
          insert into avalon.AdjustCodes(AdjustCode, AdjustDesc, reimbursable)
          select AdjustCode, AdjustDesc, 'N'
          from NewCodes ;
     quit ;

     data feesadj4 ;
          set feesadj3 ;
          UniqueKey = strip(VehicleSaleKey) || 'R' || strip(RecNum) ;
     run ;

     proc sql;
      connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
      create table feesold as select * from connection to mydb
          (
          select *
          from FeesAdj where date(modifydate)>= to_date(to_char(&backdate,'00000000'), 'yyyymmdd') /*20171013 update: pull less */
;
         )  ;
               disconnect from mydb;
      quit ;

     data feesold ;
          set feesold ;
          UniqueKey = strip(VehicleSaleKey) || 'R' || strip(RecNum) ;
     run ;

     proc sort data=feesold ;
          by UniqueKey ;
     run ;

     proc sort data=feesadj4 ;
          by UniqueKey ;
     run ;

     data NewRecs UpdateRecs ;
          merge feesold(in=a) feesadj4(in=b) ;
          by UniqueKey ;
          adjustnote = compress(adjustnote,' ()&_-.#,/\$?@:%!;+*','kni') ;        
          adjustdesc = compress(adjustdesc,' ()&_-.#,/\$?@:%!;+*','kni') ;         
          if b ;
          if a then output UpdateRecs ;
               else output NewRecs ;
     run ;

     proc compare base=feesold compare=UpdateRecs nosummary out=compdata noprint outnoequal ;
          by UniqueKey;
     run ;

     proc sql noprint ;
          create table syncfees as
          select vehiclesalekey,adjustcode,adjustdate,adjustamt,adjustnote,recnum,uniquekey,adjustdesc /*20170903 update: selected needed fields*/
          from UpdateRecs
          where UniqueKey in (select UniqueKey from compdata) ;
     quit ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute( TRUNCATE TABLE temp.syncfees ; ) by mydb ;
          disconnect from mydb ;
     quit ;

     proc append base=temp.syncfees data=syncfees ;
     run ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute(
                     UPDATE FeesAdj a
                     SET AdjustAmt = b.AdjustAmt
						,AdjustNote = b.AdjustNote
					 	,adjustcode=b.adjustcode /*20170516 update: update adjustcode too*/
                     FROM temp.syncfees b
                     WHERE a.VehicleSaleKey = b.VehicleSaleKey and a.RecNum = b.RecNum ;
                  ) by mydb ;
          disconnect from mydb ;
     quit ;


/*20170517 update: for data of common vehiclesalekey, need to delete stale data from avalon (by vehiclesalekey, if recnum is not in UpdateRecs)*/

	 proc sql;
	 create table common_vehiclesalekey as
	 select a.*
	 from feesold a
	 inner join feesadj4 b
	 	on a.vehiclesalekey=b.vehiclesalekey
	 ;
	 quit;
	 proc sql;
	 create table deletefees as
	 select distinct a.*
	 from common_vehiclesalekey a
	 where uniquekey not in (select uniquekey from feesadj4)
	 ;
	 quit;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute( TRUNCATE TABLE temp.syncfees ; ) by mydb ;
          disconnect from mydb ;
     quit ;

     proc append base=temp.syncfees data=deletefees ;
     run ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute(
					 delete from FeesAdj a
					 using temp.syncfees b
					 where a.VehicleSaleKey = b.VehicleSaleKey 
					 and a.RecNum = b.RecNum
                  ) by mydb ;
          disconnect from mydb ;
     quit ;
/*20170517 update ends*/


     proc sql noprint ;
          insert into avalon.FeesAdj(VehicleSaleKey, AdjustCode, AdjustDate, AdjustAmt, AdjustNote, RecNum)
          select VehicleSaleKey, AdjustCode, AdjustDate, AdjustAmt, AdjustNote, RecNum 
          from NewRecs ;
     quit ;

/*20170903 update:If fees processed successfully, update externalfees processed='Y'*/
				%if &syscc =0 or &syscc =4 or &syscc =1012 %then
				%do; 
					  proc sql ;
		                  connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
		                  execute(
		                           create table temp_extkeys
		                             ( externalfeeskey   integer   ) ;

		                          ) by mydb ;

		                  disconnect from mydb ;
		             quit ;

		             proc sql noprint ;
		                  insert into avalon.temp_extkeys(externalfeeskey)
		                  select externalfeeskey
		                  from external_ds_upd ;
		             quit ;

		             proc sql ;
		                  connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
		                  execute( update externalfees
		                           set Processed = 'Y'
		                           where externalfeeskey in (select externalfeeskey
		                                              from temp_extkeys)
		                           and Processed = 'N' ; ) by mydb ;
		                  disconnect from mydb ;
		             quit ;

		             proc sql ;
		                  connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
		                  execute( DROP TABLE temp_extkeys ; ) by mydb ;
		                  disconnect from mydb ;
		             quit ;

				%end;
				%else %do;
		             filename outmail email from    =('team_SAS@dealshield.com')
		                                    to      =('matthew.deal@dealshield.com' 'team_SAS@dealshield.com')
		                                    subject ="DailyUpdate encountered errors at pfdmtransync, please look into it"
		                                    ;
		             data _NULL_;
		                file outmail;
		                put "pfdmtranupdate encountered errors at Pfdmtransync";
		             run;
		             DATA _NULL_;
		                  X = SLEEP(10, 1);
		             RUN;
		             filename outmail clear;
				%end;
/*20170903 update end*/


     /* Update FloorPlanCompanies Table */

     data sflragcy(drop=dmagencyid) ;
          set pfdmtran60(keep=dmagencyid sflragcy) ;
          where not missing(dmagencyid) ;
          companycode = dmagencyid ;
     run ;

     proc sort data=sflragcy nodupkey ;
          by companycode ;
     run ;

	 /* 03/16/2017 UPDATE:  Use pass thru SQL */
     proc sql;
      connect to odbc as mydb (datasrc=tappy user=sas password=H1ghGr00v3) ;
      create table branches as select * from connection to mydb
          (
          select *
          from floor_plan_branches ;
         )  ;
               disconnect from mydb;
      quit ;

     proc sql;
      connect to odbc as mydb (datasrc=tappy user=sas password=H1ghGr00v3) ;
      create table companies as select * from connection to mydb
          (
          select *
          from floor_plan_companies ;
         )  ;
               disconnect from mydb;
      quit ;

	 /* 03/16/2017 UPDATE end:  Use pass thru SQL */

/***** REMOVED 3/16/2017 
     libname tappy odbc user=sas password=H1ghGr00v3 datasrc=tappy access=readonly ;

     data branches ;
          set tappy.floor_plan_branches ;
     run ;

     data companies ;
          set tappy.floor_plan_companies ;
     run ;

     libname tappy clear ;
*********/

	  /* 12/07/2017 UPDATE :  Include oracle_site*/
     proc sql noprint ;
          create table floorplans as
          select a.company_code as companycode, c.sflragcy, b.site_no, a.name as companyname,
                 b.name as branchname, b.address_street as streetaddress, b.address_suite as suite,
                 b.address_city as city, b.address_state as state, b.address_zipcode as zipcode,
                 a.oracle_id as oracleid, a.holds_title, a.generate_shipping_label, b.country_code, 
				 b.oracle_site as oraclesite
          from companies a left join branches b on a.company_code = b.company_code
                           left join sflragcy c on a.company_code = c.companycode ;
     quit ;

     data floorplanupdt(drop=holds_title generate_shipping_label site_no) ;
          set floorplans ;
		  suite=substr(suite, 1, 20); /*20160425 update: limit value to 20 characters*/
          if holds_title = '1' then HoldsTitle = 'Y' ;
                               else HoldsTitle = 'N' ;
          if generate_shipping_label = '1' then GenerateLabel = 'Y' ;
                                           else GenerateLabel = 'N' ;
          companysite = site_no * 1 ;
     run ;

     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute( TRUNCATE TABLE temp.floorplanupdt ; ) by mydb ;
          disconnect from mydb ;
     quit ;

     proc append base=temp.floorplanupdt data=floorplanupdt ;
     run ;

 	 /* 12/07/2017 UPDATE :  Insert new records*/
     proc sql ;
          connect to odbc as mydb (datasrc="&dbref" user=rdsuser password=!4ne12N0) ;
          execute(
                     UPDATE floorplancompanies a
                     SET sflragcy = coalesce(b.sflragcy,a.sflragcy),
                         companyname = b.companyname, branchname = b.branchname,
                         streetaddress = b.streetaddress, suite = b.suite, city = b.city, state = b.state,
                         zipcode = b.zipcode, oracleid = b.oracleid, holdstitle = b.holdstitle,
                         generatelabel = b.generatelabel, countrycode = b.country_code
                     FROM temp.floorplanupdt b
                     WHERE a.companycode = b.companycode and a.companysite = b.companysite ;


		  			 INSERT INTO into floorplancompanies a
					 		(companycode,sflragcy,companysite,companyname,branchname
							,streetaddress,suite,city,state,zipcode,oracleid,holdstitle
							,generatelabel,countrycode,oraclesite)
					 Select  companycode,sflragcy,companysite,companyname,branchname
							,streetaddress,suite,city,state,zipcode,oracleid,holdstitle
							,generatelabel,countrycode,oraclesite
					 FROM temp.floorplanupdt b
					 WHERE a.companycode <> b.companycode and a.companysite <> b.companysite ;
                  ) by mydb ;

          disconnect from mydb ;
     quit ;

%mend PfdmtranSync ;
