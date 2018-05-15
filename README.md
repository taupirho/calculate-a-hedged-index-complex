# index-hedging
A JAVA program to hedge an index in a forward currency
A set of JAVA programs that will calculate the return of an existing index/portfolio in a hedged 
1 month forward currency. This example is very dependent on the stucture of
your equity database but essentially you'll need a set of table that will enable you to get 
any portfolio sub-indices,together with their market caps and underlying currencies. from this 
you can calculate the weight of each sub-index within the overall portfolio. You will also need the 
sub-indices and hedging currencies 1 month forward and spot rates and the unhedged performance of the 
overall portfolio.

Once you've go all this info you basically do the following

hedged performance = unhedged performance + SUM(individual sub-indices hedge impacts)
