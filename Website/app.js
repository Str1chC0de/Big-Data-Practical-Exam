//Use express and mysql2
const express = require('express');
const mysql = require('mysql2');

//create app to send post and get requests
const app = express();

//Database arguments to create connection to mysql Database
const db = mysql.createConnection({
   host: 'mysql-server',
   user: 'root',
   password: 'bootroot42',
   database: 'xkcd_database'
})

//get HTML file for the Website
app.get('/', (req, res) => {
   res.sendFile(__dirname + "/index.html");
});

//get JS file of the website to build a dynamic front end
app.get('/json', (req, res) => {
   res.sendFile(__dirname + "/index.js");
});

//import Json to express
app.use(express.json());



//Create database connection
db.connect(err => {
   if(err) {
      throw err
   }
   console.log('Database connection is working now')
})


//POST Request to get Data from MySQL Database
app.post('/xkcd_search', (req, res) => {
   //get the word or word part from the request body
   var word_part = req.body.text_part;

   db.query(
      //SQL Statement order by the numbers increasing
      `SELECT * FROM xkcd_datatable WHERE title LIKE \"%${word_part}%\" ORDER BY num ASC`, function (error, result, fields) { 
      
         if (error) {
            //Retrun an error
            res.status(409).send({
               "message": error
            });
            //Return the result from db
         } else {
            res.status(200).send({
               result: result
            });
       }
   });
});


//Start Backend on intern port 3000
app.listen(3000, () => {
   console.log("My Rest API ist running on port 3000!")
});