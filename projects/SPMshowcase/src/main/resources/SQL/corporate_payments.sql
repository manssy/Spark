with op AS (
    select AccountDB, AccountCR, Amount * Rate as amt, Comment, DateOp
    from (
             select AccountDB,
                    AccountCR,
                    Amount,
                    Rate,
                    o.Comment,
                    DateOp,
                    rank() over (partition by AccountDB, o.DateOp, o.currency order by r.ratedate desc) as rss
             from operation o
                      join rate r on o.Currency = r.Currency and o.DateOp >= r.RateDate)
    where rss = 1
),
     acc as (
         select AccountID, AccountNum, ClientId, CutoffDt
         from account a
                  join (select distinct AccountDB as Account, DateOp as CutoffDt
                        from operation
                        union
                        select distinct AccountCR as Account, DateOp as CutoffDt
                        from operation) t
                       on a.AccountID = t.Account
     ),

     PaymentAmt as (
         select AccountDB, round(sum(amt), 2) as PaymentAmt, DateOp
         from op
         group by AccountDB, DateOp
     ),
     EnrollmentAmt as (
         select AccountCR, round(sum(amt), 2) as EnrollmentAmt, DateOp
         from op
         group by AccountCR, DateOp
     ),

     amt AS (
         select AccountID,
                AccountNum,
                ClientId,
                CutoffDt,
                AccountDB,
                PaymentAmt,
                AccountCR,
                EnrollmentAmt
         from acc a
                  left join PaymentAmt p on p.AccountDB = a.AccountID and a.CutoffDt = p.DateOp
                  left join EnrollmentAmt e on e.AccountCR = a.AccountID and a.CutoffDt = e.DateOp
     ),

     TaxAmt as (
         select AccountDB, PaymentAmt as TaxAmt
         from amt
         where AccountNum LIKE '40702%'
     ),
     ClearAmt as (
         select AccountCR, EnrollmentAmt as ClearAmt
         from amt
         where AccountNum LIKE '40802%'
     ),

     CarsAmt as (
         select AccountDB, PaymentAmt as CarsAmt
         from amt
         where AccountDB not in (
             select distinct AccountDB
             from op o
                      join (select val from techTable t where name = 'list1') t on o.comment like t.val
         )
     ),
     FoodAmt as (
         select AccountCR, EnrollmentAmt as FoodAmt
         from amt
         where AccountCR not in (
             select distinct AccountCR
             from op o
                      join (select val from techTable where name = 'list2') t on o.comment like t.val
         )
     ),
     FLAmt as (
         select AccountDB, PaymentAmt as FLAmt
         from amt a
                  join client c on a.ClientId = c.ClientId
         where c.type = 'Ô'
     )

select AccountID,
       ClientID,
       nvl(PaymentAmt, 0)     as PaymentAmt,
       nvl(EnrollmentAmt, 0) as EnrollementAmt,
       nvl(TaxAmt, 0)         as TaxAmt,
       nvl(ClearAmt, 0)       as ClearAmt,
       nvl(CarsAmt, 0)        as CarsAmt,
       nvl(FoodAmt, 0)        as FoodAmt,
       nvl(FLAmt, 0)          as FLAmt,
       CutoffDt
from amt a
         left join TaxAmt t on a.AccountDB = t.AccountDB
         left join ClearAmt cl on a.AccountCR = cl.AccountCR
         left join CarsAmt ca on a.AccountDB = ca.AccountDB
         left join FoodAmt f on a.AccountCR = f.AccountCR
         left join FLAmt fl on a.AccountDB = fl.AccountDB
