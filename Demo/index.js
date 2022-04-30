function data(){
    data = {
        name:"Hackathon",
        traces: "none",
        activities: "none",
    }
    return data
}

methods = {
    async loadData(name){
        url = `http://127.0.0.1:5003/loaddata?name=${name}`
        console.log(url)

        element = document.querySelector('#loadData');
        element.classList.add('lighten-2')
        let response = await fetch(url);
        let data = await response.json();
        console.log(data)
        element.classList.remove('lighten-2')
        //also disable the button
    },

    async clusterData(){
        url = `http://127.0.0.1:5003/cluster`
        console.log(url)

        element = document.querySelector('#clusterData');
        element.classList.add('lighten-2')
        let response = await fetch(url);
        let data = await response.json();
        console.log(data)
        element.classList.remove('lighten-2')
        //also disable the button
    },

    async getTraces(){
        url = `http://127.0.0.1:5003/variantstable`
        console.log(url)

        element = document.querySelector('#traces');
        element.classList.add('lighten-2')
        let response = await fetch(url);
        let data = await response.json();
        console.log(data.data)
        this.traces = data.data 
        element.classList.remove('lighten-2')
        //also disable the button
    },

    async getActivitiesCount(){
        url = `http://127.0.0.1:5003/activitiescounts`
        console.log(url)

        element = document.querySelector('#activitiesCount');
        element.classList.add('lighten-2')
        let response = await fetch(url);
        let data = await response.json();
        this.activities = data.data 
        console.log({...this.activities})
        //console.log(data.data)
        element.classList.remove('lighten-2')
        //also disable the button
    },
}

const app = Vue.createApp({
    data,
    methods:methods,
})

app.mount("#app")