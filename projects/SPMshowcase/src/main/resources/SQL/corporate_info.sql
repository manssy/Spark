select c.ClientId,
       c.ClientName,
       c.Type,
       c.Form,
       c.RegisterDate,
       round(sum(a.TotalAmt), 2) as TotalAmt,
       a.CutoffDt
from client c
         join corporate_account a on c.ClientId = a.ClientId
group by c.ClientId, c.ClientName, c.Type, c.Form, c.RegisterDate, a.CutoffDt