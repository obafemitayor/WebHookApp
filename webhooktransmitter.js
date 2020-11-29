const axios = require('axios');

const broadcastmessagetoSubscribers = (model) => 
{
    var allsubscribers = model.subscribers;
    for (let i = 0; i < allsubscribers.length; i++) 
    {
        var endpoint = allsubscribers[i];
        axios.post(endpoint, {message: model.message,});
    };
};
  
  process.on('message', (model) => {
    broadcastmessagetoSubscribers(model);
  });