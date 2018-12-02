<template>
    <el-table
            :data="tableData"
            stripe
            border
            style="width: 100%"
            height="100%"
            :row-class-name="tableRowClassName"


    >
        <el-table-column
                v-for=" (value, key) in tableData[0]"
                :prop="key"
                :label="key"
        >
        </el-table-column>
    </el-table>
</template>

<script>
    function getRandomInt(min, max) {
        min = Math.ceil(min);
        max = Math.floor(max);
        return Math.floor(Math.random() * (max - min)) + min; //The maximum is exclusive and the minimum is inclusive
    }
    
    Date.prototype.toIsoString = function() {
        var tzo = -this.getTimezoneOffset(),
            dif = tzo >= 0 ? '+' : '-',
            pad = function(num) {
                var norm = Math.floor(Math.abs(num));
                return (norm < 10 ? '0' : '') + norm;
            };
        return this.getFullYear() +
            '-' + pad(this.getMonth() + 1) +
            '-' + pad(this.getDate()) +
            'T' + pad(this.getHours()) +
            ':' + pad(this.getMinutes()) +
            ':' + pad(this.getSeconds()) +
            dif + pad(tzo / 60) +
            ':' + pad(tzo % 60);
    }


    import Vue from 'vue';
    import event from '../eventBus';

    export default Vue.extend({
        name: 'DBTable',
        props: ['table'],
        methods: {
            tableRowClassName({row, rowIndex}) {
                const now =  Date.parse(new Date().toISOString().slice(0, 19));
                console.log(now - Date.parse(row.created_at))
                if (now - Date.parse(row.created_at) < 3 *  1000) {

                    return 'success-row';
                }
                return '';
            }
        },
        mounted() {
            event.on("new-data", (d)=> {
                console.log(d)
            })
            // if (this.table === "result") {
                // this.highlight = 2;
                // this.tableData.unshift({
                //     ads_id: "1011",
                //     user_id: "14914",
                //     created_at: new Date().toISOString().slice(0, 19),
                //     title: "ADS 3"
                // },)
                // setInterval(() => {
                //     Array.from({length: getRandomInt(1,4)}, (x,i) => {
                //         this.tableData.unshift({
                //             ads_id: "1011",
                //             user_id: "14914",
                //             created_at: new Date().toISOString().slice(0, 19),
                //             title: "ADS 3"
                //         },)
                //     });
                //
                //
                //
                // }, 1000)
            // }
        },
        data() {


            let DB = {
                "user": [
                    {ads_id: "1001", title: "ADS 1", status: "normal", created_at: "2018-01-01T01:01:01"},
                    {ads_id: "1002", title: "ADS 2", status: "normal", created_at: "2018-01-01T01:01:01"},
                    {ads_id: "1003", title: "ADS 3", status: "abnormal", created_at: "2018-01-01T01:01:01"},
                    {ads_id: "1004", title: "ADS 4", status: "normal", created_at: "2018-01-01T01:01:01"},
                    {ads_id: "1005", title: "ADS 5", status: "abnormal", created_at: "2018-01-01T01:01:01"},
                    {ads_id: "1006", title: "ADS 6", status: "normal", created_at: "2018-01-01T01:01:01"},
                    {ads_id: "1007", title: "ADS 7", status: "normal", created_at: "2018-01-01T01:01:01"},
                    {ads_id: "1008", title: "ADS 8", status: "normal", created_at: "2018-01-01T01:01:01"},
                ],
                "kafka-stream": [
                    {ads_id: "1003", user_id: "14914", created_at: "0000-00-00T00:00:00"}
                ],
                "result": [
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},
                    {ads_id: "1001", user_id: "14914", created_at: "0000-00-00T00:00:00", title: "ADS 3"},

                ]
            }
            return {
                highlight: 0,
                tableData: DB[this.table]
            }
        },
    });
</script>
<style>

    .el-table td, .el-table th {
        padding: 6px 0 !important;
    }

    /*.el-table.no-drag{*/
    /*background-color: #eff1f6;*/
    /*}*/

    .el-table--striped .el-table__body tr.el-table__row.el-table__row--striped td {
        background: #eff1f65c;
    }

    .el-table--enable-row-hover .el-table__body tr:hover > td {
        background-color: rgba(173, 183, 204, 0.13) !important;
    }

    .el-table .success-row {
        background: #f0f9eb;
        -webkit-transition: background-color 1000ms linear;
        -ms-transition: background-color 1000ms linear;
        transition: background-color 1000ms linear;
    }
</style>
