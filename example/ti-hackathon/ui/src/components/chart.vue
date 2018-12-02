<template>
    <div class="chart">
        <div id="chart"></div>
    </div>
</template>

<script>
    export default {
        name: 'DBChart',
        mounted() {
            let data = {
                columns: [
                    ['Mary', 3, 2, 1, 4, 3, 9, 14],
                    ['Bob', 10, 3, 2, 1, 4, 3, 9],
                ]
            }
            let chart = c3.generate({
                bindto: '#chart',
                data: data,
                axis: {
                    y: {
                        max: 15,
                        min:0
                    }
                },
                grid: {
                    y: {
                        lines: [
                            {value: 2, text: 'normal'},
                            {value: 10, text: 'error', class: "error"}
                        ]
                    }
                },
                regions: [
                    {axis: 'y', start: 10, class: 'error'}
                    ]
            });
            chart.resize()
            let append = () => {
                    chart.resize()
                    chart.flow( {
                        columns: [
                            ['Mary', 3],
                            ['Bob', 11],
                        ]
                    })
                setTimeout(()=>{
                    append()
                }, 1000)
            }
            append()


        },
    }
</script>

<style>
    .error > line {
        stroke: #ff0000;
    }
    #chart, .chart {
        width: 100%;
        height: 100%;
        max-height: 100%!important;
    }
    .error.c3-region{
        fill-opacity: 0.2!important;
    }
    .error.c3-region{
        fill:red;
    }
</style>
