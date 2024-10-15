from dash import Dash, dcc, html
from exp_theta_two_parts import \
    violin_cmp_2_exp_wait_v_node_count, \
    violin_cmp_2_exp_wait_v_walltime, \
    violin_cmp_2_exp_boslo_v_node_count, \
    violin_cmp_2_exp_boslo_v_walltime
from dash import Dash, html, dcc, Input, Output, callback



app = Dash()
# figs = homogeneous_break_down()
# figs_het = heterogeneous_break_down(1.3, 0.5)
# figs_het2 = heterogeneous_break_down(1.1, 0.5)
app.layout = html.Div([
    dcc.RangeSlider(0, 12, 0.5, value=[0, 12], id='my-range-slider'),
    html.Div(id='graphs'),

])

@callback(
    Output('graphs', 'children'),
    Input('my-range-slider', 'value'))
def update_output(value):
    start = 0
    end = 31500000

    # start_c = ((end - start)/12)*value[0]
    # end_c = ((end - start)/12)*value[1]
    # figs = homogeneous_break_down(start=start_c, end=end_c)
    # figs_hom_nt = homogeneous_break_down_nt(start=start_c, end=end_c)
    # figs_het = heterogeneous_break_down(1.3, 0.5, start=start_c, end=end_c)
    # figs_het_nt = heterogeneous_break_down_nt(1.3, 0.5, start=start_c, end=end_c)
    # figs_het2 = heterogeneous_break_down(1.1, 0.5, start=start_c, end=end_c)
    return dcc.Tabs([
        dcc.Tab(label='Turnaround vs Random (Homogeneous)', children=[
            *violin_cmp_2_exp_wait_v_node_count(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "Turnaround",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_wait_v_walltime(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "Turnaround",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_node_count(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "Turnaround",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_walltime(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "Turnaround",
                exp_2_name = "Random"
            )
        ]),

        # dcc.Tab(label='turnaround vs theta', children=[
        #     dcc.Markdown(
        #                 figs['turnaround']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['turnaround']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['turnaround']['counts_vs_proc']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['turnaround']['counts_vs_proc']['fig']
        #     ),
        #      dcc.Markdown(
        #                 figs['turnaround']['wait_delta_vs_proc']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['turnaround']['wait_delta_vs_proc']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['turnaround']['counts_vs_walltime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['turnaround']['counts_vs_walltime']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['turnaround']['wait_delta_vs_runtime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['turnaround']['wait_delta_vs_runtime']['fig']
        #     ),
        # ]),
        # dcc.Tab(label='random vs theta', children=[
        #     dcc.Markdown(
        #                 figs['random']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['random']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_proc']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['random']['counts_vs_proc']['fig']
        #     ), 
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_walltime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs['random']['counts_vs_walltime']['fig']
        #     )
        # ]),
        # dcc.Tab(label='turnaround vs random', children=[
        #     dcc.Markdown(
        #                 figs_hom_nt['turnaround_v_random']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_hom_nt['turnaround_v_random']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs_hom_nt['turnaround_v_random']['counts_vs_proc']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_hom_nt['turnaround_v_random']['counts_vs_proc']['fig']
        #     ),
        #      dcc.Markdown(
        #                 figs_hom_nt['turnaround_v_random']['wait_delta_vs_proc']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_hom_nt['turnaround_v_random']['wait_delta_vs_proc']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs_hom_nt['turnaround_v_random']['counts_vs_walltime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_hom_nt['turnaround_v_random']['counts_vs_walltime']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs_hom_nt['turnaround_v_random']['wait_delta_vs_runtime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_hom_nt['turnaround_v_random']['wait_delta_vs_runtime']['fig']
        #     ),
        # ]),
        #         dcc.Tab(label='1.3x - user 50 ', children=[
        #     dcc.Markdown(
        #                 figs_het['user 1.3 0.5']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_het['user 1.3 0.5']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_proc']['md']
        #     ),
        #     dcc.Graph(
        #         figure= figs_het['user 1.3 0.5']['counts_vs_proc']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_walltime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_het['user 1.3 0.5']['counts_vs_walltime']['fig']
        #     )
        # ]),
        # dcc.Tab(label='Heterogeneous 1.3x - turnaround ', children=[
        #     dcc.Markdown(
        #                 figs_het['turnaround 1.3']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure=figs_het['turnaround 1.3']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_proc']['md']
        #     ),
        #     dcc.Graph(
        #         figure= figs_het['turnaround 1.3']['counts_vs_proc']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_walltime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_het['turnaround 1.3']['counts_vs_walltime']['fig']
        #     )
        # ]),
        # dcc.Tab(label='Heterogeneous 1.3x - turnaround (vs 1x) ', children=[
        #     dcc.Markdown(
        #                 figs_het_nt['turnaround 1.3']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure=figs_het_nt['turnaround 1.3']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs_het_nt['turnaround 1.3']['counts_vs_proc']['md']
        #     ),
        #     dcc.Graph(
        #         figure= figs_het_nt['turnaround 1.3']['counts_vs_proc']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs_het_nt['turnaround 1.3']['counts_vs_walltime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_het_nt['turnaround 1.3']['counts_vs_walltime']['fig']
        #     )
        # ]),
        # dcc.Tab(label='Heterogeneous 1.1x - turnaround ', children=[
        #     dcc.Markdown(
        #                 figs_het2['turnaround 1.1']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure=figs_het2['turnaround 1.1']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_proc']['md']
        #     ),
        #     dcc.Graph(
        #         figure= figs_het2['turnaround 1.1']['counts_vs_proc']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_walltime']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_het2['turnaround 1.1']['counts_vs_walltime']['fig']
        #     )
        # ]),
        # dcc.Tab(label='1.1x - user 50 ', children=[
        #     dcc.Markdown(
        #                 figs_het2['user 1.1 0.5']['wait_delta_vs_submit']['md']
        #             ),
        #     dcc.Graph(
        #         figure= figs_het2['user 1.1 0.5']['wait_delta_vs_submit']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_proc']['md']
        #     ),
        #     dcc.Graph(
        #         figure= figs_het2['user 1.1 0.5']['counts_vs_proc']['fig']
        #     ),
        #     dcc.Markdown(
        #                 figs['random']['counts_vs_walltime']['md']
        #     ),
        #     dcc.Graph(
        #         figure= figs_het2['user 1.1 0.5']['counts_vs_walltime']['fig']
        #     )
        # ])
    ],  id="my-tabs")

if __name__ == '__main__':
    app.run(debug=True)