const variants_table = null
const activities_count = null
var clusters_overview = null
var numclusters = null


document.addEventListener('DOMContentLoaded', function() {
    var elems = document.querySelectorAll('select');
    var instances = M.FormSelect.init(elems);
  });

//FUNCTIONS

async function loadData(name){
    url = `http://127.0.0.1:5003/loaddata?name=${name}`
    console.log(url)

    //element = document.querySelector('#loadData');
    //element.classList.add('lighten-2')
    mobisBtn.classList.add('yellow', 'lighten-3')
    let response = await fetch(url);
    let data = await response.json();
    console.log(data)
    mobisBtn.classList.remove('yellow', 'lighten-3')
    //element.classList.remove('lighten-2')
    //also disable the button
}

async function clusterData(){
    url = `http://127.0.0.1:5003/cluster`
    console.log(url)

    //element = document.querySelector('#clusterData');
    //element.classList.add('lighten-2')
    mobisBtn.classList.add('yellow', 'lighten-3')
    let response = await fetch(url);
    let data = await response.json();
    console.log(data)
    mobisBtn.classList.remove('yellow', 'lighten-3')
    //element.classList.remove('lighten-2')
    //also disable the button
}

async function getVariantsTable(){
    url = `http://127.0.0.1:5003/variantstable`
    console.log(url)

    //element = document.querySelector('#traces');
    mobisBtn.classList.add('yellow', 'lighten-3')
    let response = await fetch(url);
    let data = await response.json();
    console.log(data.data)
    //variants_table = data.data 
    mobisBtn.classList.remove('yellow', 'lighten-3')
    //also disable the button
}

async function setup(name){

    url1 = `http://127.0.0.1:5003/loaddata?name=${name}`
    url2 = `http://127.0.0.1:5003/cluster`
    url3 = `http://127.0.0.1:5003/variantstable`
    url4 = `http://127.0.0.1:5003/topclustersactivities`
    url5 = `http://127.0.0.1:5003/numclusters`

    if(name == "MobIS")
        mobisBtn.classList.add('yellow', 'lighten-3')

    if(name == "SAP P2P")
        p2pBtn.classList.add('yellow', 'lighten-3')
    

    let response1 = await fetch(url1);
    let data1 = await response1.json();
    //console.log(data1)

    let response2 = await fetch(url2);
    let data2 = await response2.json();
    //console.log(data2)

    let response3 = await fetch(url3);
    let data3 = await response3.json();
    //console.log(data3)

    let response4 = await fetch(url4);
    let data4 = await response4.json();
    //console.log(data4.data)

    let response5 = await fetch(url5);
    let data5 = await response5.json();
    numclusters = data5.data

    clusters_overview = data4.data;

    if(name == "MobIS")
        mobisBtn.classList.remove('yellow', 'lighten-3')

    if(name == "SAP P2P")
        p2pBtn.classList.remove('yellow', 'lighten-3')

}

async function getActivitiesCount(){
    url = `http://127.0.0.1:5003/activitiescounts`
    console.log(url)

    element = document.querySelector('#activitiesCount');
    element.classList.add('lighten-2')
    let response = await fetch(url);
    let data = await response.json();
    activities_count = data.data 
    console.log(activities_count)
    //console.log(data.data)
    element.classList.remove('lighten-2')
    //also disable the button
}

function report_overview(){
    document.querySelector("#numclusters").innerHTML = `${numclusters}`
}
//MAIN

const mobisBtn = document.querySelector("#mobis")
const p2pBtn = document.querySelector("#p2p")

mobisBtn.addEventListener("mouseenter", (e) => {
    if (!mobisBtn.classList.contains('green')){
        mobisBtn.classList.add('grey', 'lighten-3')
    }
        
});

mobisBtn.addEventListener("mouseleave", (e) => {
    if (!mobisBtn.classList.contains('green'))
        mobisBtn.classList.remove('grey', 'lighten-3')
});


mobisBtn.addEventListener("click", (e) => {
    if (p2pBtn.classList.contains('green', 'lighten-4'))
        p2pBtn.classList.remove('green', 'lighten-4')

    mobisBtn.classList.remove('grey', 'lighten-3')
    setup("MobIS").then(d=>{
        console.log("im done")
        console.log(numclusters)
        report_overview();
    });
    //loadData("MobIS");
    //clusterData();
    //getVariantsTable();
    mobisBtn.classList.add('green' ,'lighten-4')
});

p2pBtn.addEventListener("mouseenter", (e) => {
    if (!p2pBtn.classList.contains('green')){
        p2pBtn.classList.add('grey', 'lighten-3')
    }
        
});

p2pBtn.addEventListener("mouseleave", (e) => {
    if (!p2pBtn.classList.contains('green'))
        p2pBtn.classList.remove('grey', 'lighten-3')
});

p2pBtn.addEventListener("click", (e) => {
    if (mobisBtn.classList.contains('green', 'lighten-4'))
        mobisBtn.classList.remove('green', 'lighten-4')

    p2pBtn.classList.remove('grey', 'lighten-3')
    setup("SAP P2P").then(d=>{
        console.log("im done")
        console.log(numclusters)
        report_overview();
    });
    //loadData("SAP P2P");
    //clusterData();
    p2pBtn.classList.add('green' ,'lighten-4')
});

// document.querySelector("#variants").addEventListener("click", (e) => {
//     getVariantsTable();
// })




