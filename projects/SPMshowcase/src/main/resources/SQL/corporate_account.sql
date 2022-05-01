select p.AccountId,
       AccountNum,
       DateOpen,
       p.ClientId,
       ClientName,
       PaymentAmt + EnrollementAmt as TotalAmt,
       CutoffDt
from corporate_payments p
         join account a on p.AccountId = a.AccountId
         join client c on p.ClientId = c.ClientId
