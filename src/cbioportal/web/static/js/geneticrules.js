// Standalone genetic rulesets — ported from src/shared/components/oncoprint/geneticrules.ts
// All color constants inlined; no build step required.
// Sets window.geneticrules with the same exports as the app-level geneticrules.ts
(function () {
    // ── color constants (from AlterationColors.ts + Colors.ts) ───────────────
    var DEFAULT_GREY = '#BEBEBE';
    var MUT_COLOR_MISSENSE = '#008000';
    var MUT_COLOR_MISSENSE_PASSENGER = '#53D400';
    var MUT_COLOR_INFRAME = '#993404';
    var MUT_COLOR_INFRAME_PASSENGER = '#a68028';
    var MUT_COLOR_TRUNC = '#000000';
    var MUT_COLOR_TRUNC_PASSENGER = '#708090';
    var MUT_COLOR_SPLICE = '#e5802b';
    var MUT_COLOR_SPLICE_PASSENGER = '#f0b87b';
    var MUT_COLOR_PROMOTER = '#00B7CE';
    var MUT_COLOR_PROMOTER_PASSENGER = '#8cedf9';
    var MUT_COLOR_OTHER = '#cf58bc';
    var MUT_COLOR_OTHER_PASSENGER = '#f96ae3';
    var MUT_COLOR_GERMLINE = '#FFFFFF';
    var MRNA_COLOR_HIGH = '#ff9999';
    var MRNA_COLOR_LOW = '#6699cc';
    var PROT_COLOR_HIGH = '#ff3df8';
    var PROT_COLOR_LOW = '#00E1FF';
    var CNA_COLOR_AMP = '#ff0000';
    var CNA_COLOR_GAIN = '#ffb6c1';
    var CNA_COLOR_HETLOSS = '#8fd8d8';
    var CNA_COLOR_HOMDEL = '#0000ff';
    var STRUCTURAL_VARIANT_COLOR = '#8B00C9';
    var STRUCTURAL_VARIANT_PASSENGER_COLOR = '#ce92e8';

    // ── hexToRGBA (from shared/lib/Colors.ts) ────────────────────────────────
    function hexToRGBA(str) {
        var s = str.replace('#', '');
        if (s.length === 3) s = s[0]+s[0]+s[1]+s[1]+s[2]+s[2];
        var r = parseInt(s.slice(0,2),16);
        var g = parseInt(s.slice(2,4),16);
        var b = parseInt(s.slice(4,6),16);
        var a = s.length === 8 ? parseInt(s.slice(6,8),16)/255 : 1;
        return [r,g,b,a];
    }

    // ── legend order constants ────────────────────────────────────────────────
    var MUTATION_LEGEND_ORDER = 0;
    var STRUCTURAL_VARIANT_LEGEND_ORDER = 1;
    var GERMLINE_LEGEND_ORDER = 2;
    var AMP_LEGEND_ORDER = 10;
    var GAIN_LEGEND_ORDER = 11;
    var HOMDEL_LEGEND_ORDER = 12;
    var HETLOSS_LEGEND_ORDER = 13;
    var MRNA_HIGH_LEGEND_ORDER = 20;
    var MRNA_LOW_LEGEND_ORDER = 21;
    var PROT_HIGH_LEGEND_ORDER = 31;
    var PROT_LOW_LEGEND_ORDER = 32;

    // ── shape bank ───────────────────────────────────────────────────────────
    var shapeBank = {
        defaultGrayRectangle: { type:'rectangle', fill:hexToRGBA(DEFAULT_GREY), z:1 },

        ampRectangle:    { type:'rectangle', fill:hexToRGBA(CNA_COLOR_AMP),    x:0,y:0,width:100,height:100,z:2 },
        gainRectangle:   { type:'rectangle', fill:hexToRGBA(CNA_COLOR_GAIN),   x:0,y:0,width:100,height:100,z:2 },
        homdelRectangle: { type:'rectangle', fill:hexToRGBA(CNA_COLOR_HOMDEL), x:0,y:0,width:100,height:100,z:2 },
        hetlossRectangle:{ type:'rectangle', fill:hexToRGBA(CNA_COLOR_HETLOSS),x:0,y:0,width:100,height:100,z:2 },

        mrnaHighRectangle:{ type:'rectangle', fill:[0,0,0,0], stroke:hexToRGBA(MRNA_COLOR_HIGH), 'stroke-width':2, x:0,y:0,width:100,height:100,z:3 },
        mrnaLowRectangle: { type:'rectangle', fill:[0,0,0,0], stroke:hexToRGBA(MRNA_COLOR_LOW),  'stroke-width':2, x:0,y:0,width:100,height:100,z:3 },

        protHighRectangle:{ type:'rectangle', fill:hexToRGBA(PROT_COLOR_HIGH), x:0,y:0,  width:100,height:20,z:4 },
        protLowRectangle: { type:'rectangle', fill:hexToRGBA(PROT_COLOR_LOW),  x:0,y:80, width:100,height:20,z:4 },

        structuralVariantDriverRectangle:{ type:'rectangle', fill:hexToRGBA(STRUCTURAL_VARIANT_COLOR),           x:0,y:20,width:100,height:60,z:5 },
        structuralVariantVUSRectangle:   { type:'rectangle', fill:hexToRGBA(STRUCTURAL_VARIANT_PASSENGER_COLOR), x:0,y:20,width:100,height:60,z:5 },

        germlineRectangle:{ type:'rectangle', fill:hexToRGBA(MUT_COLOR_GERMLINE), x:0,y:46,width:100,height:8,z:7 },

        missenseMutationDriverRectangle:  { type:'rectangle', fill:hexToRGBA(MUT_COLOR_MISSENSE),           x:0,y:33.33,width:100,height:33.33,z:6 },
        missenseMutationVUSRectangle:     { type:'rectangle', fill:hexToRGBA(MUT_COLOR_MISSENSE_PASSENGER), x:0,y:33.33,width:100,height:33.33,z:6 },
        inframeMutationDriverRectangle:   { type:'rectangle', fill:hexToRGBA(MUT_COLOR_INFRAME),            x:0,y:33.33,width:100,height:33.33,z:6 },
        inframeMutationVUSRectangle:      { type:'rectangle', fill:hexToRGBA(MUT_COLOR_INFRAME_PASSENGER),  x:0,y:33.33,width:100,height:33.33,z:6 },
        truncatingMutationDriverRectangle:{ type:'rectangle', fill:hexToRGBA(MUT_COLOR_TRUNC),              x:0,y:33.33,width:100,height:33.33,z:6 },
        truncatingMutationVUSRectangle:   { type:'rectangle', fill:hexToRGBA(MUT_COLOR_TRUNC_PASSENGER),    x:0,y:33.33,width:100,height:33.33,z:6 },
        spliceMutationDriverRectangle:    { type:'rectangle', fill:hexToRGBA(MUT_COLOR_SPLICE),             x:0,y:33.33,width:100,height:33.33,z:6 },
        spliceMutationVUSRectangle:       { type:'rectangle', fill:hexToRGBA(MUT_COLOR_SPLICE_PASSENGER),   x:0,y:33.33,width:100,height:33.33,z:6 },
        promoterMutationDriverRectangle:  { type:'rectangle', fill:hexToRGBA(MUT_COLOR_PROMOTER),           x:0,y:33.33,width:100,height:33.33,z:6 },
        promoterMutationVUSRectangle:     { type:'rectangle', fill:hexToRGBA(MUT_COLOR_PROMOTER_PASSENGER), x:0,y:33.33,width:100,height:33.33,z:6 },
        otherMutationDriverRectangle:     { type:'rectangle', fill:hexToRGBA(MUT_COLOR_OTHER),              x:0,y:33.33,width:100,height:33.33,z:6 },
        otherMutationVUSRectangle:        { type:'rectangle', fill:hexToRGBA(MUT_COLOR_OTHER_PASSENGER),    x:0,y:33.33,width:100,height:33.33,z:6 },
    };

    // ── shared non-mutation rules (CNA, mRNA, protein) ────────────────────────
    var non_mutation_conditional = {
        disp_cna: {
            'amp_rec,amp':         { shapes:[shapeBank.ampRectangle],     legend_label:'Amplification',    legend_order:AMP_LEGEND_ORDER    },
            'gain_rec,gain':       { shapes:[shapeBank.gainRectangle],    legend_label:'Gain',             legend_order:GAIN_LEGEND_ORDER   },
            'homdel_rec,homdel':   { shapes:[shapeBank.homdelRectangle],  legend_label:'Deep Deletion',    legend_order:HOMDEL_LEGEND_ORDER },
            'hetloss_rec,hetloss': { shapes:[shapeBank.hetlossRectangle], legend_label:'Shallow Deletion', legend_order:HETLOSS_LEGEND_ORDER},
        },
        disp_mrna: {
            high: { shapes:[shapeBank.mrnaHighRectangle], legend_label:'mRNA High', legend_order:MRNA_HIGH_LEGEND_ORDER },
            low:  { shapes:[shapeBank.mrnaLowRectangle],  legend_label:'mRNA Low',  legend_order:MRNA_LOW_LEGEND_ORDER  },
        },
        disp_prot: {
            high: { shapes:[shapeBank.protHighRectangle], legend_label:'Protein High', legend_order:PROT_HIGH_LEGEND_ORDER },
            low:  { shapes:[shapeBank.protLowRectangle],  legend_label:'Protein Low',  legend_order:PROT_LOW_LEGEND_ORDER  },
        },
    };

    var always_rule = {
        shapes: [shapeBank.defaultGrayRectangle],
        legend_label: 'No alterations',
        legend_order: Infinity,
    };

    var base_params = {
        type: 'GENE',
        legend_label: 'Genetic Alteration',
        na_legend_label: 'Not profiled',
        legend_base_color: hexToRGBA(DEFAULT_GREY),
    };

    function assign() {
        var result = {};
        for (var i = 0; i < arguments.length; i++) {
            var src = arguments[i];
            for (var k in src) { if (src.hasOwnProperty(k)) result[k] = src[k]; }
        }
        return result;
    }

    // ── SV rules ──────────────────────────────────────────────────────────────
    var sv_no_recurrence = {
        disp_structuralVariant: {
            'sv_rec,sv': { shapes:[shapeBank.structuralVariantDriverRectangle], legend_label:'Structural Variant', legend_order:STRUCTURAL_VARIANT_LEGEND_ORDER },
        },
    };
    var sv_recurrence = {
        disp_structuralVariant: {
            sv_rec: { shapes:[shapeBank.structuralVariantDriverRectangle], legend_label:'Structural Variant (putative driver)',      legend_order:STRUCTURAL_VARIANT_LEGEND_ORDER },
            sv:     { shapes:[shapeBank.structuralVariantVUSRectangle],    legend_label:'Structural Variant (unknown significance)', legend_order:STRUCTURAL_VARIANT_LEGEND_ORDER },
        },
    };

    // ── exported rule sets ────────────────────────────────────────────────────

    var genetic_rule_set_different_colors_no_recurrence = assign({}, base_params, {
        rule_params: {
            always: always_rule,
            conditional: assign({}, non_mutation_conditional, sv_no_recurrence, {
                disp_mut: {
                    'other,other_rec':       { shapes:[shapeBank.otherMutationDriverRectangle],      legend_label:'Other Mutation',      legend_order:MUTATION_LEGEND_ORDER },
                    'promoter,promoter_rec': { shapes:[shapeBank.promoterMutationDriverRectangle],   legend_label:'Promoter Mutation',   legend_order:MUTATION_LEGEND_ORDER },
                    'splice,splice_rec':     { shapes:[shapeBank.spliceMutationDriverRectangle],     legend_label:'Splice Mutation',     legend_order:MUTATION_LEGEND_ORDER },
                    'trunc,trunc_rec':       { shapes:[shapeBank.truncatingMutationDriverRectangle], legend_label:'Truncating Mutation', legend_order:MUTATION_LEGEND_ORDER },
                    'inframe,inframe_rec':   { shapes:[shapeBank.inframeMutationDriverRectangle],    legend_label:'Inframe Mutation',    legend_order:MUTATION_LEGEND_ORDER },
                    'missense,missense_rec': { shapes:[shapeBank.missenseMutationDriverRectangle],   legend_label:'Missense Mutation',   legend_order:MUTATION_LEGEND_ORDER },
                },
            }),
        },
    });

    var genetic_rule_set_different_colors_recurrence = assign({}, base_params, {
        rule_params: {
            always: always_rule,
            conditional: assign({}, non_mutation_conditional, sv_recurrence, {
                disp_mut: {
                    other_rec:    { shapes:[shapeBank.otherMutationDriverRectangle],      legend_label:'Other Mutation (putative driver)',         legend_order:MUTATION_LEGEND_ORDER },
                    other:        { shapes:[shapeBank.otherMutationVUSRectangle],         legend_label:'Other Mutation (unknown significance)',    legend_order:MUTATION_LEGEND_ORDER },
                    promoter_rec: { shapes:[shapeBank.promoterMutationDriverRectangle],   legend_label:'Promoter Mutation (putative driver)',      legend_order:MUTATION_LEGEND_ORDER },
                    promoter:     { shapes:[shapeBank.promoterMutationVUSRectangle],      legend_label:'Promoter Mutation (unknown significance)', legend_order:MUTATION_LEGEND_ORDER },
                    splice_rec:   { shapes:[shapeBank.spliceMutationDriverRectangle],     legend_label:'Splice Mutation (putative driver)',        legend_order:MUTATION_LEGEND_ORDER },
                    splice:       { shapes:[shapeBank.spliceMutationVUSRectangle],        legend_label:'Splice Mutation (unknown significance)',   legend_order:MUTATION_LEGEND_ORDER },
                    trunc_rec:    { shapes:[shapeBank.truncatingMutationDriverRectangle], legend_label:'Truncating Mutation (putative driver)',    legend_order:MUTATION_LEGEND_ORDER },
                    trunc:        { shapes:[shapeBank.truncatingMutationVUSRectangle],    legend_label:'Truncating Mutation (unknown significance)',legend_order:MUTATION_LEGEND_ORDER },
                    inframe_rec:  { shapes:[shapeBank.inframeMutationDriverRectangle],    legend_label:'Inframe Mutation (putative driver)',       legend_order:MUTATION_LEGEND_ORDER },
                    inframe:      { shapes:[shapeBank.inframeMutationVUSRectangle],       legend_label:'Inframe Mutation (unknown significance)',  legend_order:MUTATION_LEGEND_ORDER },
                    missense_rec: { shapes:[shapeBank.missenseMutationDriverRectangle],   legend_label:'Missense Mutation (putative driver)',      legend_order:MUTATION_LEGEND_ORDER },
                    missense:     { shapes:[shapeBank.missenseMutationVUSRectangle],      legend_label:'Missense Mutation (unknown significance)', legend_order:MUTATION_LEGEND_ORDER },
                },
            }),
        },
    });

    var genetic_rule_set_same_color_for_all_no_recurrence = assign({}, base_params, {
        rule_params: {
            always: always_rule,
            conditional: assign({}, non_mutation_conditional, sv_no_recurrence, {
                disp_mut: {
                    'splice,trunc,inframe,missense,promoter,other,splice_rec,trunc_rec,inframe_rec,missense_rec,promoter_rec,other_rec': {
                        shapes:[shapeBank.missenseMutationDriverRectangle], legend_label:'Mutation', legend_order:MUTATION_LEGEND_ORDER,
                    },
                },
            }),
        },
    });

    var genetic_rule_set_same_color_for_all_recurrence = assign({}, base_params, {
        rule_params: {
            always: always_rule,
            conditional: assign({}, non_mutation_conditional, sv_recurrence, {
                disp_mut: {
                    'splice_rec,missense_rec,inframe_rec,trunc_rec,promoter_rec,other_rec': {
                        shapes:[shapeBank.missenseMutationDriverRectangle], legend_label:'Mutation (putative driver)',      legend_order:MUTATION_LEGEND_ORDER,
                    },
                    'splice,missense,inframe,trunc,promoter,other': {
                        shapes:[shapeBank.missenseMutationVUSRectangle], legend_label:'Mutation (unknown significance)', legend_order:MUTATION_LEGEND_ORDER,
                    },
                },
            }),
        },
    });

    window.geneticrules = {
        genetic_rule_set_different_colors_no_recurrence:   genetic_rule_set_different_colors_no_recurrence,
        genetic_rule_set_different_colors_recurrence:      genetic_rule_set_different_colors_recurrence,
        genetic_rule_set_same_color_for_all_no_recurrence: genetic_rule_set_same_color_for_all_no_recurrence,
        genetic_rule_set_same_color_for_all_recurrence:    genetic_rule_set_same_color_for_all_recurrence,
    };
})();
