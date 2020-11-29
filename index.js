const fs = require('fs');
const { fork } = require('child_process');
var express = require('express');
const bodyParser = require("body-parser");
const util = require('util');
const writeFile = util.promisify(fs.writeFile);
var app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());


app.post('/subscribe/:topic',  function(req, res)
{
//get topic from url
var topic = req.params.topic;
var data = '';
//create readable file stream to read hashtabe from file
var readStream = fs.createReadStream('webhookhashtabe.txt', 'utf8');
//read contents on the file
readStream.on('data', function(chunk) 
{
    data += chunk;
})

//process data gotten from file
readStream.on('end', async function() 
{
    //create hashtable variable
    var subscribtionhashtable = {};
    //create webhookist array
    var webhookist = new Array();
    //check if data is empty
    if (data === "") 
    {
        //Data is empty
        //push url to webhookist
        webhookist.push(req.body.url);
        //add topic to the hashtable and assign webhooklist as its value
        subscribtionhashtable[topic] = webhookist;
    }
    else
    {
        //Data is not empty....Parse json data to object
        subscribtionhashtable = JSON.parse(data);
        //check if topic exist in hashtable
        if(topic in obj)
        {
            //topic exist, get webhooklist value
            webhookist = subscribtionhashtable[topic];
            //add url to list
            webhookist.push(req.body.url);
            //update topic in hashtable
            subscribtionhashtable[topic] = webhookist;
        }
        else
        {
            //topic does not exist
            //populate list
            webhookist.push(req.body.url);
            //create topic in hashtabe
            subscribtionhashtable[topic] = webhookist;
        }
    }
    //Stringify New HashTable
    var stringdata = JSON.stringify(subscribtionhashtable);
    //Update File Contents
    await writeFile('webhookhashtabe.txt', stringdata);
});
//return response
 res.send("Subscribtion Added");
 });


 app.post('/publish/:topic', function(req, res)
{
    //get topic from url
    var topic = req.params.topic;
    var data = '';
    //create readable file stream to read hashtabe from file
    var readStream = fs.createReadStream('webhookhashtabe.txt', 'utf8');
    //read contents on the file
    readStream.on('data', function(chunk) 
{
    data += chunk;
})

    //process data gotten from file
    readStream.on('end', async function() 
    {
    console.log(data);
    var subscribtionhashtable = {};
    var webhooklist = new Array();
    //check if data is empty
    if (data === "") 
    {
        //return error
        res.send("Topic Does Not Exist...");
    }
    else
    {
        //Data is not empty....Parse json data to object
        subscribtionhashtable = JSON.parse(data);
        //check if topic exist in hashtabe
        if(topic in obj)
        {
            webhooklist = subscribtionhashtable[topic];

            //Because there could be many subscribers for a topic, use parallel programming approach to broadcast
            //Get all subcribers and split in batches of 20
            var webhooklistcount = webhooklist.length;
            var lastindex = webhooklistcount - 1;
            var batchsize = 20;
            var iterationcount = webhooklistcount/batchsize;
            const brodcastevent = fork('webhooktransmitter.js');
            //For each batch spin a child process to handle the broadcasting to each subscriber
            for(var i = 0; i < iterationcount; i++)
            {
                var subscribersbatch = new Array();
                var counter = 0;
                var startindex = i * batchsize;
                //Build batch list
                while(counter < batchsize)
                {
                    if(startindex > lastindex)
                    {
                    subscribersbatch.push(startindex);
                    startindex++;
                    counter++;
                    }
                    else
                    {
                        break;
                    }
                }
                if(subscribersbatch.length > 0)
                {
                //Build model for child process
                var model = 
                {
                    message : req.body.message,
                    subscribers : subscribersbatch
                }
                //send to child process
                brodcastevent.send(model);
                }
            }
        }
        else
        {
            //return error
            res.send("Topic Does Not Exist...");
        }
    }
    });
    res.send("Event has been broadcasted to subscribers");
 });


app.listen(3000);
