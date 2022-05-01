function report_overview(){
    document.querySelector("#overview").classList.remove('hide')
    document.querySelector("#numclusters").innerHTML = `${numclusters}`
    //console.log(clusters_overview);
    var c = 1
    var a = 1
    var cluster_arr = []
    Object.entries(clusters_overview).forEach(([key,val]) => {
        //console.log(`${key}, ${val}`);


        if (key !== -1 && val['count'] !== null && c<=3 ){
            document.querySelector(`#c${c}-n`).innerHTML = `${val['count']}`
            cluster_arr.push(`${key}`)
            
            Object.entries(val['activities']).forEach(([key,val]) => {
                console.log( `${val[0]}`.substring(0,20));
                if(a<=4){
                    document.querySelector(`#c${c}a${a}`).innerHTML = `${val[0]}`.substring(0,15)
                    a = a+1
                }
               })
    
            a = 1
            c = c+1

        }
        if (key == -1) {
            document.querySelector("#cn-n").innerHTML = `${val['count']}`
            cluster_arr.push(`${key}`)

            Object.entries(val['activities']).forEach(([key,val]) => {
                console.log( `${val[0]}`.substring(0,20));
                if(a<=4){
                    document.querySelector(`#cna${a}`).innerHTML = `${val[0]}`.substring(0,15)
                    a = a+1
                }
               })
    
            a = 1
       }


        //console.log(key, val['count']);

        
    });
    throughput_time = report_throughput_time(cluster_arr);

}

async function report_throughput_time(cluster_arr){
    //console.log(cluster_arr.join())
    url = `http://127.0.0.1:5003/throughput?clusters=${cluster_arr.join()}`
    //console.log(url)
    
    let response = await fetch(url);
    let data = await response.json();

    var c = 1
    data.data.forEach(item => {
        //console.log(item[0], item[1])
        document.querySelector(`#c${c}-t`).innerHTML = `${item[1]}`
        c = c+1
    })

    //console.log(data.data)

}
