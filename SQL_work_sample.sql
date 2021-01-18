with base as
(select
    rnid, -- Added RnID for easier joining in later queries
    opid,
    oporid,
	RnRniID,
    CASE WHEN returntypename='Reroute' or RnRniID IS NOT NULL THEN 'Reroute' ELSE 'In Customer Possession' END AS returntypename,
	CASE WHEN spclassid =1 THEN 'SP' ELSE 'LP' END AS shipping_type,
	CASE WHEN RnDestinationID=705 Then 1071 ELSE RnDestinationID END AS RnDestinationID,
    returnenddestination,
    returnservicelevelname,
    pickupcarriertypename,
    pickupcarriername,
    cast(convert(char(8),firstreceiptdate) as date) AS first_receipt_date,
    cast(convert(char(8),orderdateconcat) as date) as order_date,
    coalesce(cast(convert(char(8),returninitiationdateconcat) as date),cast(RnDateNew as date)) as return_initiation_date,
    datediff(DAY,cast(convert(char(8),orderdateconcat) as date),cast(convert(char(8),returninitiationdateconcat) as date)) as order_return_datedif,
    datediff(DAY,cast(convert(char(8),returninitiationdateconcat) as date),cast(convert(char(8),firstreceiptdate) as date)) as return_close_datedif,
    sum(fr.rnqty) as return_quantity,
    RnClosed,
    Rncancelled,
    coalesce(slm.return_sva,slm.return_su,sva.svaname,su.suname,'Unknown') as destination,
	coalesce(slm.slmfromname,slm.svaname, pickupcarriername,'Unknown') as starting_sva,
    slm.*
from csn_reporting_bi.dbo.rn_factreturn fr WITH (NOLOCK)
         INNER JOIN csn_reporting_bi..rn_dimReturn dr with (NOLOCK) on fr.ReturnKey = dr.ReturnKey
         LEFT JOIN csn_order..tblreturn r WITH (NOLOCK) on rnopid=fr.opid
         LEFT JOIN csn_order..tblplserviceagent sva WITH (NOLOCK) on sva.svaid=r.RnDestinationID and r.RnDestinationType=2
         LEFT JOIN csn_order..tblSupplier su WITH (NOLOCK) on su.suid=r.RnDestinationID and r.RnDestinationType=1
         OUTER APPLY
		(
         SELECT top 1 slmid,oplid,slmfromname,sva.svaname,rsva.svaname as return_sva,rsu.suname as return_su,
					  OplIsDispose,OplIsLiquidation,opl.OplLastEventID
         FROM csn_order..tblOverpackItem opi WITH (NOLOCK)
                  INNER JOIN csn_order..tbloverpackload opl WITH (NOLOCK) on opi.OpiOlID=OplID
                  INNER JOIN csn_order..tblplserviceagent sva WITH (NOLOCK) on sva.svaid=OplSvaID
                  LEFT JOIN csn_order..tblShippingLoadManifest slm WITH (NOLOCK) on slm.slmid=opl.OplSlmOutID -- Converted this to a left join to dig into OPLs that exist but simply don't have an outbound SLM assigned
				  LEFT JOIN csn_order..tblplserviceagent rsva WITH (NOLOCK) on rsva.svaid=OplReturnSvaID		 
				  LEFT JOIN csn_order..tblSupplier rsu WITH (NOLOCK) on rsu.suid=OplReturnSuID
		 where opi.opiopid=fr.opid
         order by OplID desc
     ) slm  --final leg of shipping information
where
        fr.oprecyid=1
		AND cast(orcompletedate as date) between dateadd(DAY,-500, GETDATE()) and dateadd(DAY,-1,GETDATE())
		AND spclassid in (1,2,3)
		AND RnCancelled=0
		AND Coalesce(slm.OplIsDispose,0) = 0
	    AND Coalesce(slm.OplIsLiquidation,0) = 0
		AND NOT EXISTS (
        SELECT TOP 1 1
        FROM csn_order.dbo.tblShippingEvent se WITH (NOLOCK)
        WHERE 1=1
          AND se.ShipID = slm.OplLastEventID
          AND se.ShipActionID IN (143, 358, 396, 480, 361, 363, 375, 473, 482)
    )
group by rnid,
	RnRniID,
    opid,
    oporid,
    returntypename,
    returnenddestination,
    returnservicelevelname,
    pickupcarriertypename,
    pickupcarriername,
	sva.svaname,
	su.suname,
	RnClosed,
	slm.OplIsDispose,
	slm.OplIsLiquidation,
	CASE WHEN RnDestinationID=705 Then 1071 ELSE RnDestinationID END,
    CASE WHEN spclassid =1 THEN 'SP' ELSE 'LP' END,
    datediff(DAY,cast(convert(char(8),orderdateconcat) as date),cast(convert(char(8),returninitiationdateconcat) as date)),
    cast(convert(char(8),orderdateconcat) as date),
    coalesce(cast(convert(char(8),returninitiationdateconcat) as date),cast(RnDateNew as date)),
    cast(convert(char(8),firstreceiptdate) as date),
    RnClosed,
    coalesce(sva.svaname,su.suname,'Unknown'),
    slmid,oplid,slmfromname,slm.svaname,slm.return_sva,slm.return_su,
	slm.OplLastEventID,
    returninitiationdateconcat,
    Rncancelled)
select *
from base
where returntypename ='Reroute'
