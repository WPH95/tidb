<template>
    <div class="editor-context">
        <div id="editor" class="ace_editor">
        </div>
        <div class="controls">
            <el-button type="primary"  round size="mini">submit</el-button>
        </div>
    </div>

</template>

<script>
    import Vue from 'vue';
    import event from "../eventBus"

    export default Vue.extend({
        name: 'editor',
        mounted() {
            let data = {
                columns: [
                    ['Mary', 3, 2, 1, 4, 3, 9, 14],
                    ['Bob', 10, 3, 2, 1, 4, 3, 9],
                ]
            }


            let ace = window.ace;
            let editor = ace.edit("editor");
            editor.setTheme("ace/theme/solarized_light");
            editor.session.setMode("ace/mode/sql");
            editor.setOptions({
                enableBasicAutocompletion: true,
                enableLiveAutocompletion: true
            });
            event.on("grid-resize", ()=> {
                console.log("recivied")
                editor.resize()
            });
            // editor.setValue("SELECT\n" +
            //     "  user,\n" +
            //     "  TUMBLE_END(\n" +
            //     "    cTime,\n" +
            //     "    \"1 hours\") # interval\n" +
            //     "    AS endT,\n" +
            //     "  COUNT(url) AS cnt\n" +
            //     "FROM clicks\n" +
            //     "GROUP BY\n" +
            //     "  TUMBLE(\n" +
            //     "    cTime,\n" +
            //     "    \"1 hours\"),\n" +
            //     "  user", 1)


            editor.setValue("SELECT\n" +
                "  k.ads_id,\n" +
                "  k.user_id,\n" +
                "  k.created_at,\n" +
                "  a.title,\n" +
                "FROM kafka_stream as k\n" +
                "JOIN ads as a\n" +
                "WHERE a.status.abnormal\n")
            window.e = editor
            editor.resize()
            setTimeout(()=>editor.resize(), 500)

        },
    });
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style>
    .controls {
        position: absolute;
        top: 10px;
        right: 10px;
    }
    .editor-context {
        height: 100%;
        width: 100%;
    }
    .vue-resizable-handle {
        z-index: 1000;
    }
    #editor {
        height: 100%;
        width: 100%;
    }
    .ace_editor {
        border: none;
        background-color: #eff1f6;
    }

    .ace_editor .ace_gutter {
        color: rgba(0,0,0,0.2);
        background-color: rgba(173, 183, 204, 0.07);
    }

    .ace_editor .ace_cursor {
        color: #45A5FF;
    }

    .ace_editor .ace_marker-layer .ace_active-line,
    .ace_editor .ace_gutter-active-line {
        background-color: rgba(255,255,255,0.6);
    }
</style>
