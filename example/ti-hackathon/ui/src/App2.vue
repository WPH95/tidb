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
                :margin="[0, 0]"
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
                <DBTable v-if="item.type ==='table'" class="no-drag"></DBTable>
                <DBChart v-if="item.type ==='chart'" class="no-drag"></DBChart>
            </grid-item>
        </grid-layout>


        <!--<div class="chart">-->
        <!--<div id="chart"></div>-->
        <!--</div>-->
    </div>
</template>

<script>
    import VueGridLayout from 'vue-grid-layout';
    import event from './eventBus'
    import DBTable from './components/table'
    import Editor from './components/editor.vue'
    import DBChart from "./components/chart";

    export default {
        name: 'page2',
        components: {
            DBChart,
            Editor,
            DBTable,
            GridLayout: VueGridLayout.GridLayout,
            GridItem: VueGridLayout.GridItem
        },
        mounted() {

        },
        data() {

            var testLayout = [
                {"x": 2, "y": 0, "w": 8, "h": 10, "i": "0", type: "editor"},
                {"x": 0, "y": 10, "w": 6, "h": 10, "i": "2", type: "table"},
                {"x": 6, "y": 10, "w": 6, "h": 10, "i": "3", type: "chart"}
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
