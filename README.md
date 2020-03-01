# mysql-promisified

It's literally just [mysql](https://www.npmjs.com/package/mysql) with promise wrappers, so take a look at their docs.
If the original callback has multiple arguments, an array will be returned.

So for example, you can do this with queries.

```
const mysql      = require('mysql-promisified');
const connection = mysql.createConnection(...);

const [results, fields] = await connection.query('SELECT 1');
```
