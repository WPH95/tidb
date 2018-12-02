<template>
    <div id="app">
        <grid-layout
                :layout.sync="layout"
                :col-num="12"
                :row-height="30"
                :is-draggable="true"
                :is-resizable="true"
                :is-mirrored="false"
                :vertical-compact="true"
                :margin="[10, 10]"
                :use-css-transforms="true"
                @resize="resizeEvent"


        >

            <grid-item v-for="item in layout"
                       :x="item.x"
                       :y="item.y"
                       :w="item.w"
                       :h="item.h"
                       :i="item.i"
                       drag-allow-from=".vue-draggable-handle"
                       drag-ignore-from=".no-drag"
                       @resize="resizeEvent">
                <div class="vue-draggable-handle"></div>
                    <Editor v-if="item.type ==='editor'" class="no-drag"></Editor>
                    <DBTable v-if="item.type ==='table'" class="no-drag" v-bind="item.props"></DBTable>
                    <DBChart v-if="item.type ==='chart'" class="no-drag"></DBChart>
            </grid-item>
        </grid-layout>


        <!--<div class="chart">-->
        <!--<div id="chart"></div>-->
        <!--</div>-->
    </div>
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

    import VueGridLayout from 'vue-grid-layout';
    import event from './eventBus'
    import DBTable from './components/table'
    import Editor from './components/editor.vue'
    import DBChart from "./components/chart";

    export default {
        name: 'page1',
        components: {
            DBChart,
            Editor,
            DBTable,
            GridLayout: VueGridLayout.GridLayout,
            GridItem: VueGridLayout.GridItem
        },
        mounted() {
            let fn = ()=> {
                let newdata = []
                Array.from({length: getRandomInt(1, 4)}, (x, i) => {
                    newdata.unshift({
                        ads_id: "1011",
                        user_id: "14914",
                        created_at: new Date().toISOString().slice(0, 19),
                        title: "ADS 3"
                    },)
                });
                event.emit("new-data", newdata);
            }

            setTimeout(function repeat() {
                fn();
                setTimeout(repeat, 1000);
            }, 1000);

        },
        data() {

            var testLayout = [
                {"x": 0, "y": 0, "w": 4, "h": 10, "i": "0", type: "table", props: {table: "user"}},
                {"x": 0, "y": 5, "w": 4, "h": 8, "i": "1", type: "editor"},
                {"x": 0, "y": 10, "w": 4, "h": 10, "i": "2", type: "table", props: {table: "kafka-stream"}},
                {"x": 5, "y": 0, "w": 6, "h": 15, "i": "3", type: "table", props: {table: "result"}},
            ];
            return {

                layout: testLayout
            }
        },
        methods: {
            resizeEvent: function (i, newH, newW, newHPx, newWPx) {
                event.emit("grid-resize")
                console.log("send resize")
            }
        }
    }
</script>

<style>
    html, body, #app {
        margin: 0;
        font-family: 'Lato', sans-serif !important;
        height: 100%;
        width: 100%;
    }

    .c3 svg {
        font: 10px 'Lato', sans-serif;
    }

    .error > line {
        stroke: #ff0000;
    }

    .vue-draggable-handle {
        z-index: 1000;
        position: absolute;
        width: 20px;
        height: 20px;
        top: 0;
        left: 0;
        /*background: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10'><circle cx='5' cy='5' r='5' fill='#999999'/></svg>") no-repeat;*/
        background-position: bottom right;
        padding: 0 8px 8px 0;
        background-repeat: no-repeat;
        background-origin: content-box;
        box-sizing: border-box;
        cursor: pointer;
    }
</style>
