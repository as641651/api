document.querySelector("#visualize").addEventListener("click", (e) => {
    data = get_data_for_graphs()
})

async function get_data_for_graphs(){
    url = `http://127.0.0.1:5003/throughput-all`

    let response = await fetch(url);
    let data = await response.json();

    console.log(data.data)
    return data.data
}

function prepare_data(data){
    g_data = []
    g_data.push({
        name:'root',
        parent:'',
    })

    Object.entries(data).forEach(([key,val]) => {
        g_data.push({
            name: key,
            val:val,
            parent:'root'
        })
    })

    console.log(g_data)

}