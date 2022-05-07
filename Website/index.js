//clear dive tabel
function clear_table()
{
    //get table body
    const parent = document.getElementById("table_body");

    //while row child exist
    while (parent.firstChild)
    {
        //remove rows from table_body
        parent.firstChild.remove();
    }
}

function request_comics() {
    //call clear function
    clear_table();

    //get text from input box
    let search_text_part = document.getElementById("comic_name").value;
    
    //convert text to json format for post request
    var input_as_json = {
        "text_part": search_text_part
    }
    
    //HTTP Request via XMLHttpRequest()
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open("POST", "http://34.107.59.15:5000/xkcd_search", false);
    xmlHttp.setRequestHeader("Content-Type", "application/json"); 
    //send the word/word part as json
    xmlHttp.send(JSON.stringify(input_as_json));

    //HTTP Response
    var response_json = xmlHttp.response;

    //parse respone from json to object
    var response_object = JSON.parse(response_json);

    //create object list
    var response_list = response_object.result;


    //Erstellen der Tabelle in welcher das ergebnis angezeigt wird
    table_body = document.getElementById("table_body");

    //loop for each element in the response_list
    for (var i = 0; i < response_list.length; i++) {
        var list_element = response_list[i];

        //add a new row to the table
        var row = document.createElement("tr");
        table_body.appendChild(row);

        //add 4 columns for num, text, img, link
        var column_1 = document.createElement("th");
        var column_2 = document.createElement("td");
        var column_3 = document.createElement("td");
        var column_4 = document.createElement("td");
        
        //specify Elementtype for each column
        var num = document.createElement("a")
        var text = document.createElement("a");
        var img = document.createElement("img");
        var link = document.createElement("p");

        //define content of columns
        num.textContent = list_element.num;
        text.textContent = list_element.title;
        img.src = list_element.img;

        //generate link to the xkcd comic
        var website = "https://xkcd.com/"
        var xkcd_link = document.createTextNode(website + list_element.num + "/");
        link.appendChild(xkcd_link);
        link.title = website + list_element.num + "/";
        link.href = website + list_element.num + "/";
        
        //append the content to the columns
        column_1.appendChild(num);
        column_2.appendChild(text);
        column_3.appendChild(img);
        column_4.appendChild(link);

        //append columns to the row which was added before
        row.appendChild(column_1)
        row.appendChild(column_2);
        row.appendChild(column_3);
        row.appendChild(column_4);
   }
}