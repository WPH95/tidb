<template>
    <div id="app">
        <router-view></router-view>
    </div>
</template>

<script>
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
